"""
This file is a collection of upgraded TM1 bedrock functionality, ported to python / pandas with the help of TM1py.
"""

import pandas as pd
from mdxpy import MdxBuilder, MdxHierarchySet, Member
from pandas import DataFrame
from TM1py import TM1Service
import re
from typing import Callable, List, Dict, Optional, Any, Union, Iterator


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------


def parse_from_clause(mdx_query: str) -> str:
    """
    Extracts the cube name from the FROM clause of an MDX query.

    Args:
        mdx_query (str): The MDX query string to parse.

    Returns:
        str: The name of the cube specified in the FROM clause.

    Raises:
        ValueError: If the MDX query does not contain a valid FROM clause.
    """
    from_part_match: Optional[re.Match[str]] = re.search(r"FROM\s*\[(.*?)\]", mdx_query, re.IGNORECASE)
    if not from_part_match:
        raise ValueError("MDX query is missing the FROM clause.")
    return from_part_match.group(1).strip()


def parse_where_clause(mdx_query: str) -> List[List[str]]:
    """
    Parses the WHERE clause of an MDX query and extracts the dimensions, hierarchies, and elements.

    The function first looks for elements in the format `[Dimension].[Hierarchy].[Element]`. If no hierarchy
    is specified, it assumes the hierarchy name is the same as the dimension and looks for `[Dimension].[Element]`.

    Args:
        mdx_query (str): The MDX query string to parse.

    Returns:
        List[List[str]]: A list of lists where each sublist contains three strings:
                        - The dimension name
                        - The hierarchy name (or the dimension name if no hierarchy is present)
                        - The element name
    """
    where_match: Optional[re.Match[str]] = re.search(r'WHERE\s*\((.*?)\)', mdx_query, re.S)
    if not where_match:
        return []

    where_content: str = where_match.group(1)
    hier_elements: List[tuple] = re.findall(r'\[(.*?)\]\.\[(.*?)\]\.\[(.*?)\]', where_content)
    result: List[List[str]] = [[dim, hier, elem] for dim, hier, elem in hier_elements]

    remaining_content: str = re.sub(r'\[(.*?)\]\.\[(.*?)\]\.\[(.*?)\]', '', where_content)
    dim_elements: List[tuple] = re.findall(r'\[(.*?)\]\.\[(.*?)\]', remaining_content)
    result.extend([[dim, dim, elem] for dim, elem in dim_elements])

    return result


def generate_kwargs_from_set_mdx_list(mdx_expressions: List[str]) -> Dict[str, str]:
    """
    Generate a dictionary of kwargs from a list of MDX expressions.

    Args:
        mdx_expressions (List[str]): A list of MDX expressions.

    Returns:
        Dict[str, str]: A dictionary where keys are dimension names (in lowercase, spaces removed)
            and values are the MDX expressions.
    """
    regex = r"\{\s*\[\s*([\w\s]+?)\s*\]\s*"
    return {
        re.search(regex, mdx).group(1).lower().replace(" ", ""): mdx
        for mdx in mdx_expressions
        if re.search(regex, mdx)
    }


# ------------------------------------------------------------------------------------------------------------
# Utility: Metadata building specific ID-s
# ------------------------------------------------------------------------------------------------------------

QUERY_VAL = "query value"
QUERY_FILTER_DIMS = "query filter dimensions"
QUERY_FILTER_HIER = "hierarchy"
QUERY_FILTER_ELEM = "element"
CUBE_NAME = "cube name"
CUBE_DIMS = "dimensions"
DIM_HIERS = "hierarchies"
DEFAULT_NAME = "default member name"
DEFAULT_TYPE = "default memmber type"

# ------------------------------------------------------------------------------------------------------------
# Utility: Cube metadata collection using input MDXs and/or other cubes
# ------------------------------------------------------------------------------------------------------------


class Metadata:
    """
    A recursive metadata structure that behaves like a nested dictionary. Provides methods for
    accessing, setting, iterating over keys, and converting the metadata to dictionary or list formats.

    The purpose of this class is to collect all necessary utility data for a single mdx query and/or it's cube
    for robust dataframe transformations, such as mdx filter dimensions and their elements, cube attributes,
    dimensions in cube, hierarchies of dimensions, default members of hierarchies, etc.

    This can be generated for each procedure, or generated once and then passed as value.

    - `__getitem__`: Returns the value for the given key, creating a new nested `Metadata` if the key does not exist.
    - `__setitem__`: Sets the value for a specified key.
    - `__iter__`: Returns an iterator over the keys.
    - `__repr__` / `__str__`: Provides a string representation of the metadata keys.
    - `to_dict`: Recursively converts the metadata to a dictionary.
    - `to_list`: Returns a list of the top-level keys in the metadata.
    - `get_cube_name`: Returns the cube name.
    - `get_cube_dims`: Returns the dimensions of the cube.
    - `get_filter_dims`: Returns the filter dimensions of the mdx query (that were in the WHERE clause)
    - `get_filter_elem`: Returns the exact element of a filter dimension in the mdx query (that was in the WHERE clause)

    """

    def __init__(self) -> None:
        self._data: Dict[str, Union['Metadata', Any]] = {}

    def __getitem__(self, item: str) -> Union['Metadata', Any]:
        if item not in self._data:
            self._data[item] = Metadata()
        return self._data[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __repr__(self) -> str:
        return f"Metadata({list(self._data.keys())})"

    def __str__(self) -> str:
        return repr(self)

    def to_dict(self) -> Dict[str, Any]:
        return {k: v.to_dict() if isinstance(v, Metadata) else v for k, v in self._data.items()}

    def to_list(self) -> list:
        return list(self._data.keys())

    def get_cube_name(self) -> str:
        return self[CUBE_NAME]

    def get_cube_dims(self) -> List[str]:
        return self[CUBE_DIMS].to_list()

    def get_filter_dims(self) -> List[str]:
        return self[QUERY_FILTER_DIMS].to_list()

    def get_filter_elem(self, dimension: str) -> str:
        return self[QUERY_FILTER_DIMS][dimension][QUERY_FILTER_ELEM]


def collect_metadata(
    metadata_function: Optional[Callable[..., DataFrame]] = None,
    **kwargs: Any
) -> Metadata:
    """
    Retrieves a Metadata object by executing the provided metadata function.

    Args:
        metadata_function (Optional[Callable]): A function to execute the MDX query and return a DataFrame.
                                           If None, the default function is used.
        **kwargs (Any): Additional keyword arguments passed to the MDX function.

    Returns:
        Metadata: The Metadata object resulting from the metadata function call
    """
    if metadata_function is None:
        metadata_function = collect_metadata_default

    return metadata_function(**kwargs)


def collect_metadata_default(
    tm1_service: Any,
    mdx: Optional[str] = None,
    cube_name: Optional[str] = None,
    retrieve_all_dimension_data: Optional[
        Callable[[Any, List[str], Metadata, Callable[[Any, str, List[str], Metadata], Metadata]], None]
    ] = None,
    retrieve_dimension_data: Optional[
        Callable[[Any, str, List[str], Metadata], Metadata]
    ] = None
) -> Metadata:
    """
    Collects important data about the mdx query and/or it's cube based on either an MDX query or a cube name.

    Args:
        tm1_service (Any): The TM1 service object used to interact with the cube.
        mdx (Optional[str]): The MDX query string.
        cube_name (Optional[str]): The name of the cube.
        retrieve_all_dimension_data (Optional[Callable]): A callable function to handle retrieving all dimension data.
        retrieve_dimension_data (Optional[Callable]): A callable function to handle metadata retrieval for dims.

    Returns:
        Metadata: A structured metadata object containing information about the cube.

    Raises:
        ValueError: If neither an MDX query nor a cube name is provided.
    """

    metadata = Metadata()

    if mdx:
        cube_name = parse_from_clause(mdx)
        metadata = collect_query_metadata(mdx, metadata)

    if not cube_name:
        raise ValueError("No MDX or cube name was specified.")

    metadata[CUBE_NAME] = cube_name
    cube_dimensions = tm1_service.cubes.get_dimension_names(cube_name)

    if retrieve_all_dimension_data is None:
        retrieve_all_dimension_data = retrieve_all_dimension_data_default

    if retrieve_dimension_data is None:
        retrieve_dimension_data = retrieve_dimension_data_default

    metadata = retrieve_all_dimension_data(
        tm1_service=tm1_service,
        cube_dimensions=cube_dimensions,
        metadata=metadata,
        retrieve_dimension_data=retrieve_dimension_data
    )

    return metadata


def retrieve_all_dimension_data_default(
    tm1_service: Any,
    cube_dimensions: List[str],
    metadata: Metadata,
    retrieve_dimension_data: Callable[[Any, str, List[str], Metadata], Metadata]
) -> Metadata:
    """
    Default implementation to retrieve and update metadata for all dimensions of a cube.

    Args:
        tm1_service (Any): The TM1 service object.
        cube_dimensions (List[str]): A list of dimension names in the cube.
        metadata (Metadata): The metadata object to update.
        retrieve_dimension_data (Callable): A function to retrieve and update metadata for each dimension.

    Returns:
        metadata (Metadata)
    """
    for dimension in cube_dimensions:
        dimension_hierarchies = tm1_service.hierarchies.get_all_names(dimension_name=dimension)
        retrieve_dimension_data(tm1_service, dimension, dimension_hierarchies, metadata)

    return metadata


def retrieve_dimension_data_default(
    tm1_service: Any,
    dimension: str,
    hierarchies: List[str],
    metadata: Metadata
) -> Metadata:
    """
    Default implementation to retrieve and collect metadata for a dimension and its hierarchies.

    Args:
        tm1_service (Any): The TM1 service object.
        dimension (str): The name of the dimension.
        hierarchies (List[str]): A list of hierarchies in the dimension.
        metadata (Metadata): The metadata object to update.

    Returns:
        Metadata: The updated metadata object.
    """
    for hierarchy in hierarchies:
        default_member = tm1_service.hierarchies.get(
            dimension_name=dimension, hierarchy_name=hierarchy
        ).default_member
        metadata[CUBE_DIMS][dimension][DIM_HIERS][hierarchy][DEFAULT_NAME] = default_member

        default_member_type = tm1_service.elements.get(
            dimension_name=dimension, hierarchy_name=hierarchy, element_name=default_member
        ).element_type
        metadata[CUBE_DIMS][dimension][DIM_HIERS][hierarchy][DEFAULT_TYPE] = default_member_type

    return metadata


def collect_query_metadata(mdx: str, metadata: Metadata) -> Metadata:
    """
    Extracts the filter dimensions and their elements in the mdx query (parts of the WHERE clause)

    Args:
        mdx (str): The MDX query string.
        metadata (Metadata): The metadata object to update.

    Returns:
        Metadata: The updated metadata object.
    """

    metadata["query value"] = mdx
    where_clause = parse_where_clause(mdx)

    for dimension, hierarchy, element in where_clause:
        metadata[QUERY_FILTER_DIMS][dimension][QUERY_FILTER_HIER] = hierarchy
        metadata[QUERY_FILTER_DIMS][dimension][QUERY_FILTER_ELEM] = element

    return metadata


# ------------------------------------------------------------------------------------------------------------
# Utility: DataFrame validation functions
# ------------------------------------------------------------------------------------------------------------


def validate_dataframe_columns(
    dataframe: DataFrame,
    cube_name: str,
    metadata_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> bool:
    """
    Collects the column labels from the DataFrame and compares them with the column labels collected from the Metadata
    based on a cube_name or metadata_function.
    Compares them as lists to check for matching label names and order.

    Args:
        dataframe: (DataFrame): The DataFrame to validate.
        cube_name: (srt): The name of the Cube.
        metadata_function: (Optional[Callable]): A function to collect metadata for validation.
                                                 If None, a default function is used.
        **kwargs: (Any): Additional keyword arguments passed to the function.
    Returns:
        boolean: True if the column label names and their order in the input DataFrame match the column labels
                 of the Metadata. False if the column labels or their order does not match.
    """

    metadata = collect_metadata(metadata_function=metadata_function, cube_name=cube_name, **kwargs)
    dimensions_from_metadata = metadata.get_cube_dims()
    dimensions_from_dataframe = list(map(str, dataframe.keys()))
    dimensions_from_dataframe.remove("Value")

    return dimensions_from_metadata == dimensions_from_dataframe


def validate_dataframe_values(
    dataframe: DataFrame
) -> bool:
    """
    Checks if the DataFrame rows contain NaN values in the "Value" column

    Args:
        dataframe: (DataFrame): The DataFrame to check for NaN values.
    Returns:
         boolean: True if the DataFrame does not contain NaN values.
                  False if it does.
    """

    return not dataframe["Value"].isna().values.any()


def validate_dataframe_no_duplicates(
    dataframe: DataFrame
) -> bool:
    """
    Checks if the DataFrame rows contain duplicate values for validating the DataFrame.

    Args:
        dataframe: (DataFrame): The DataFrame to check for duplicate values.
    Returns:
         boolean: True if the DataFrame does not contain duplicate values.
                  False if it does.
    """
    return not dataframe.duplicated(keep=False).any()


def validate_dataframe_rows(
    dataframe: DataFrame
) -> bool:
    """
    Checks if the DataFrame rows are valid. A row is valid if it does not contain duplicate or NaN values.

    Args:
        dataframe: (DataFrame): The DataFrame to check for duplicate and NaN values.
    Returns:
        boolean: True if the DataFrame does not contain duplicate or NaN values.
                 False if it does contain either.
    """
    return validate_dataframe_values(dataframe=dataframe) and validate_dataframe_no_duplicates(dataframe=dataframe)


def validate_dataframe(
    dataframe: DataFrame,
    cube_name: str,
    metadata_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> bool:
    """
    Calls the validate_dataframe_rows() and validate_dataframe_columns() functions and returns only returns True if both
    conditions are met.

    Args:
        dataframe: (DataFrame): The DataFrame to validate.
        cube_name: (srt): The name of the Cube.
        metadata_function: (Optional[Callable]): A function to collect metadata for validation.
                                                 If None, a default function is used.
        **kwargs: (Any): Additional keyword arguments passed to the function.
    Returns:
        boolean: True if all conditions are met.
                 False if any of the called functions returned False.
    """
    return (validate_dataframe_rows(dataframe=dataframe) and
            validate_dataframe_columns(
                dataframe=dataframe,
                cube_name=cube_name,
                metadata_function=metadata_function,
                **kwargs
            ))


def validate_dataframe_transform_input(
        source_dataframe: DataFrame,
        target_cube_name: str,
        source_dim_mapping: dict,
        related_dimensions: dict,
        target_dim_mapping: dict,
        **kwargs
) -> bool:

    source_dimensions = source_dataframe.columns()
    target_dimensions = collect_metadata(cube_name=target_cube_name, **kwargs).get_cube_dims()

    if source_dimensions == target_dimensions:
        return True
    else:
        return (
            validate_source_dim_mapping(
                source_dimensions=source_dimensions,
                source_dim_mapping=source_dim_mapping,
                related_dimensions=related_dimensions
            ) and
            validate_target_dim_mapping(
                target_dimensions=target_dimensions,
                target_dim_mapping=target_dim_mapping,
                related_dimensions=related_dimensions
            )
        )


def validate_source_dim_mapping(
        source_dimensions: list,
        source_dim_mapping: dict,
        related_dimensions: dict
) -> bool:
    related_dim_list = list(map(str, related_dimensions.keys()))
    if related_dim_list in source_dimensions:
        return True
    else:
        source_dim_list = list(map(str, source_dim_mapping.keys()))
        return source_dimensions == source_dim_list


def validate_target_dim_mapping(
        target_dimensions: list,
        target_dim_mapping: dict,
        related_dimensions: dict
) -> bool:
    related_dim_list = list(map(str, related_dimensions.values()))
    if related_dim_list in target_dimensions:
        return True
    else:
        source_dim_list = list(map(str, target_dim_mapping.keys()))
        return target_dimensions == source_dim_list


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


def mdx_to_dataframe(
    mdx_function: Optional[Callable[..., DataFrame]] = None,
    **kwargs: Any
) -> DataFrame:
    """
    Retrieves a DataFrame by executing the provided MDX function.

    Args:
        mdx_function (Optional[Callable]): A function to execute the MDX query and return a DataFrame.
                                           If None, the default function is used.
        **kwargs (Any): Additional keyword arguments passed to the MDX function.

    Returns:
        DataFrame: The DataFrame resulting from the MDX query.
    """
    if mdx_function is None:
        mdx_function = mdx_to_dataframe_default

    return mdx_function(**kwargs)


def mdx_to_dataframe_default(
    tm1_service: TM1Service,
    data_mdx: Optional[str] = None,
    data_mdx_list:  Optional[list[str]] = None,
    skip_zeros: bool = False,
    skip_consolidated_cells: bool = False,
    skip_rule_derived_cells: bool = False
) -> DataFrame:
    """
    Executes an MDX query using the default TM1 service function and returns a DataFrame.
    If an MDX is given, it will execute it synchronously,
    if an MDX list is given, it will execute them asynchronously.

    Args:
        tm1_service (TM1Service): An active TM1Service object for connecting to the TM1 server.
        data_mdx (str): The MDX query string to execute.
        data_mdx_list (list[str]): A list of mdx queries to execute in an asynchronous way.
        skip_zeros (bool, optional): If True, cells with zero values will be excluded. Defaults to False.
        skip_consolidated_cells (bool, optional): If True, consolidated cells will be excluded. Defaults to False.
        skip_rule_derived_cells (bool, optional): If True, rule-derived cells will be excluded. Defaults to False.

    Returns:
        DataFrame: A DataFrame containing the result of the MDX query.
    """

    if data_mdx_list:
        return tm1_service.cells.execute_mdx_dataframe_async(
            mdx_list=data_mdx_list,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True
        )
    else:
        return tm1_service.cells.execute_mdx_dataframe(
            mdx=data_mdx,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True
        )


def normalize_dataframe(
    dataframe: DataFrame,
    metadata_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> DataFrame:
    """
    Returns a normalized dataframe using the raw output dataframe of the execute mdx function, and necessary cube
    and query based metadata. Makes sure that all cube dimensions are present in the dataframe and that they are in
    the right order.

    Args:
        dataframe (DataFrame): The DataFrame to normalize.
        metadata_function (Optional[Callable]): A function to collect metadata for normalization.
                                                If None, a default function is used.
        **kwargs (Any): Additional keyword arguments for the metadata function.

    Returns:
        DataFrame: The normalized DataFrame.
    """



    metadata = collect_metadata(metadata_function=metadata_function, **kwargs)
    dataframe_dimensions = metadata.get_cube_dims()

    if 'Value' not in dataframe_dimensions:
        dataframe_dimensions.append('Value')

    filter_dimensions = metadata.get_filter_dims()
    additional_columns = {
        dimension: metadata.get_filter_elem(dimension) for dimension in filter_dimensions
    }

    dataframe = dataframe.assign(**additional_columns)
    dataframe = pd.concat([dataframe, DataFrame(
        columns=[name for name in dataframe_dimensions if name not in dataframe.columns]
    )], axis=1)

    return dataframe.loc[:, dataframe_dimensions]


def mdx_to_normalized_dataframe(
    mdx_function: Optional[Callable[..., DataFrame]] = None,
    metadata_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> DataFrame:
    """
    Retrieves and normalizes a DataFrame from an MDX query function. The output dataframe will be in the format of the
    cube, ready for writing.

    Args:
        mdx_function (Optional[Callable]): A function to retrieve a DataFrame from an MDX query.
                                           If None, a default function is used.
        metadata_function (Optional[Callable]): A function to collect metadata for normalization.
                                                If None, a default function is used.
        **kwargs (Any): Additional keyword arguments for the MDX and normalization functions.

    Returns:
        DataFrame: The normalized DataFrame.
    """

    dataframe = mdx_to_dataframe(mdx_function, **kwargs)
    dataframe = normalize_dataframe(dataframe, metadata_function, **kwargs)
    return dataframe


def mdx_object_builder(
        cube_name: str,
        cube_filter: dict,
        metadata_function: Optional[Callable[..., Any]] = None,
        **kwargs: Any
) -> str:
    """
    Returns a valid MDX from a cube and dictionary filters. All dimensions not specified in the filter dictionary
    will get all leaves elements put in the query.

    Args:


    Returns:
        DataFrame: The normalized DataFrame.
    """

    if metadata_function is None:
        metadata_function = collect_metadata

    metadata = metadata_function(cube_name=cube_name, **kwargs)
    dataframe_dimensions = metadata.get_cube_dims()

    mdx_object = MdxBuilder.from_cube(cube_name)
    dim_keys = [key for key in cube_filter]

    for dim in dataframe_dimensions:
        if dim not in dim_keys:
            mdx_object.add_hierarchy_set_to_axis(1, MdxHierarchySet.all_leaves(dim))
        else:
            member_keys = [key for key in cube_filter[dim].keys()]
            value = member_keys[0]
            mdx_object.add_hierarchy_set_to_axis(0, MdxHierarchySet.member(Member.of(dim, value)))

    return mdx_object.to_mdx()


# ------------------------------------------------------------------------------------------------------------
# Main: normalized pandas dataframe to cube functions
# ------------------------------------------------------------------------------------------------------------

def clear_cube(
    clear_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> None:
    """
    Clears a cube with filters. If no custom function is provided, the default function is used.

    Args:
        clear_function (Optional[Callable]): A function to clear the cube using set MDXs.
                                             Defaults to the built-in TM1 service function.
        **kwargs (Any): Additional keyword arguments for the clear function, which may include:
                        - tm1_service (TM1Service): An active TM1Service object for the server connection.
                        - cube_name (str): The name of the cube to clear.
                        - clear_set_mdx_list (List[str]): A list of valid MDX set expressions defining the clear space.
    """
    if clear_function is None:
        clear_function = clear_cube_default
    return clear_function(**kwargs)


def clear_cube_default(
    tm1_service: TM1Service,
    cube_name: str,
    clear_set_mdx_list: List[str]
) -> None:
    """
    Clears a cube with filters by generating clear parameters from a list of set MDXs.

    Args:
        tm1_service (TM1Service): An active TM1Service object for the TM1 server connection.
        cube_name (str): The name of the cube to clear.
        clear_set_mdx_list (List[str]): A list of valid MDX set expressions defining the clear space.
    """
    clearing_kwargs = generate_kwargs_from_set_mdx_list(clear_set_mdx_list)
    tm1_service.cells.clear(cube_name, **clearing_kwargs)


def dataframe_to_cube(
    write_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> None:
    """
    Writes a DataFrame to a cube. If no custom function is provided, the default function is used.

    Args:
        write_function (Optional[Callable]): A function to write the DataFrame to the cube.
                                             Defaults to the built-in TM1 service function.
        **kwargs (Any): Additional keyword arguments for the write function.
    """
    if write_function is None:
        write_function = dataframe_to_cube_default
    return write_function(**kwargs)


def dataframe_to_cube_default(
    tm1_service: TM1Service,
    dataframe: DataFrame,
    cube_name: str,
    cube_dims: List[str],
    async_write: bool = False,
    use_ti: bool = False,
    use_blob: bool = False,
    increment: bool = False,
    sum_numeric_duplicates: bool = True,
    **kwargs
) -> None:
    """
    Writes a DataFrame to a cube using the TM1 service.

    Args:
        tm1_service (TM1Service): An active TM1Service object for the TM1 server connection.
        dataframe (DataFrame): The DataFrame to write to the cube.
        cube_name (str): The name of the target cube.
        cube_dims (List[str]): A list of dimensions for the target cube.
        async_write (bool, optional): Whether to write data asynchronously. Defaults to False.
        use_ti (bool, optional): Whether to use TurboIntegrator. Defaults to False.
        use_blob (bool, optional): Whether to use the 'blob' method. Defaults to False.
        increment (bool, optional): Increments the values in the cube instead of replacing them. Defaults to False.
        sum_numeric_duplicates (bool, optional): Aggregate numerical values for duplicated intersections.
            Defaults to True.

    Returns:
        None
    """
    function_name = "write_dataframe_async" if async_write else "write_dataframe"

    getattr(tm1_service.cells, function_name)(
        cube_name=cube_name,
        data=dataframe,
        dimensions=cube_dims,
        deactivate_transaction_log=True,
        reactivate_transaction_log=True,
        skip_non_updateable=True,
        use_ti=use_ti,
        use_blob=use_blob,
        increment=increment,
        sum_numeric_duplicates=sum_numeric_duplicates
    )


def dataframe_to_cube_with_clear(
    clear_function: Optional[Callable[..., None]] = None,
    write_function: Optional[Callable[..., None]] = None,
    clear_target: bool = False,
    **kwargs: Any
) -> None:
    """
    Clears a cube and writes a DataFrame to it if requested.

    Args:
        clear_function (Optional[Callable]): A function to clear the cube. Defaults to None.
        write_function (Optional[Callable]): A function to write the DataFrame to the cube.
                                             Must not be None.
        clear_target (bool, optional): If True, the cube will be cleared before writing. Defaults to False.
        **kwargs (Any): Additional keyword arguments for the clear and write functions.

    Returns:
        None
    """
    if clear_target:
        clear_cube(clear_function, **kwargs)
    dataframe_to_cube(write_function, **kwargs)


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe transform utility functions
# ------------------------------------------------------------------------------------------------------------


def dataframe_filter(
    dataframe: DataFrame,
    filter_condition: Dict[str, Any],
    inplace: bool = False
) -> DataFrame:
    """
    Filters a DataFrame based on a given filter_condition.

    - If at least one valid condition is met, it filters the DataFrame.
    - If no valid condition is met, it returns an empty DataFrame.
    - If 'inplace' is True, the function modifies the original DataFrame and returns it.
    - If 'inplace' is False (default), it returns a new filtered DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame to filter.
        filter_condition (Dict[str, Any]): Dictionary with column names as keys and values to filter for.
        inplace (bool, optional): If True, modifies the original DataFrame in-place. Defaults to False.

    Returns:
        DataFrame: The filtered DataFrame (either new or modified in-place).
    """
    valid_columns = list(filter(lambda col: col in dataframe.columns, filter_condition.keys()))

    # in case of inplace=True, don't return empty data
    if not valid_columns:
        return dataframe if inplace else dataframe.iloc[0:0]

    # build condition boolean string for row dropping
    condition = dataframe[valid_columns].eq(
        pd.Series({col: filter_condition[col] for col in valid_columns})
    ).all(axis=1)

    return dataframe.drop(index=dataframe.index[~condition]).reset_index(drop=True) if inplace else dataframe.loc[condition]


def dataframe_drop_column(
        dataframe: DataFrame,
        column_list: list[str]
) -> DataFrame:
    """
    Drops columns from DataFrame if the values in the input column_list are found in the columns of the DataFrame.
    If a column_list value is not found in the DataFrame, it is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame from which columns are to be dropped.
        column_list: (list): Name of the columns to be dropped.
    Returns:
        DataFrame: The updated DataFrame.
    """
    if not all(item in dataframe.columns for item in column_list):
        return dataframe
    return dataframe.drop(column_list, axis=1).reset_index(drop=True)


def dataframe_add_column_assign_value(
        dataframe: DataFrame,
        column_value: dict
) -> DataFrame:
    """
    Ads columns with assigned values to DataFrame if the column_value pairs are not found in the DataFrame.
    If a column from the column_value pair is found in the DataFrame, the pair is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame to which columns are to be added.
        column_value: (dict): Column:value pairs to be added.
    Returns:
        DataFrame: The updated DataFrame.
    """
    new_columns = {col: value for col, value in column_value.items() if col not in dataframe.columns}

    if new_columns:
        dataframe[list(new_columns)] = DataFrame([new_columns], index=dataframe.index)

    return dataframe.reset_index(drop=True)


def dataframe_redimension_scale_down(
        dataframe: DataFrame,
        filter_condition: dict,
) -> DataFrame:
    """
    Filters DataFrame based on filter_condition and drops columns given in column_list.
    Only filters the DataFrame if at least one condition is met. If non is met, it returns an empty DataFrame.

    Args:
        dataframe: (DataFrame): The DataFrame to filter.
        filter_condition: (dict) Dimension:element key,value pairs for filtering the DataFrame.
    Returns:
        DataFrame: The updated DataFrame.
    """

    filtered_dataframe = dataframe_filter(dataframe=dataframe, filter_condition=filter_condition)
    column_list = list(map(str, filter_condition.keys()))
    return dataframe_drop_column(dataframe=filtered_dataframe, column_list=column_list).reset_index(drop=True)


def dataframe_drop_zero_and_values(
        dataframe: DataFrame
) -> DataFrame:
    """
    Drops all rows with zero values from DaraFrame, then drops the values column.

    Args:
        dataframe: (DataFrame): The DataFrame to update.
    Return:
        DataFrame: The updated DataFrame without the zero values.
    """

    dataframe.drop(dataframe[dataframe["Value"] == 0].index, inplace=True)
    dataframe.drop(columns=["Value"], inplace=True)
    return dataframe.reset_index(drop=True)


def dataframe_relabel(
        dataframe: DataFrame,
        columns: dict
) -> DataFrame:
    """
    Relabels DataFrame column(s) if the original label is found in the DataFrame.
    If an original label is not found, then it is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame to relabel.
        columns: (dict): The original and the new column labels as key-value pairs.
                         The key stands for the original column label, the value for the new label.
    Return: None
    """
    dataframe.rename(columns=columns, inplace=True)
    return dataframe


def dataframe_value_scale(
        dataframe: DataFrame,
        value_function: callable
) -> DataFrame:
    """
    Applies an input function to the 'Value' column of the DataFrame.

    Args:
        dataframe (DataFrame): The input DataFrame.
        value_function (callable): A function to apply to the 'Value' column.

    Returns:
        DataFrame: The modified DataFrame (in place).
    """
    dataframe["Value"] = dataframe["Value"].apply(value_function)
    return dataframe.reset_index(drop=True)


def dataframe_redimension_and_transform(
        dataframe: DataFrame,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None
) -> DataFrame:

    if source_dim_mapping is not None:
        dataframe = dataframe_redimension_scale_down(dataframe=dataframe, filter_condition=source_dim_mapping)

    if related_dimensions is not None:
        dataframe = dataframe_relabel(dataframe=dataframe, columns=related_dimensions)

    if target_dim_mapping is not None:
        dataframe = dataframe_add_column_assign_value(dataframe=dataframe, column_value=target_dim_mapping)

    return dataframe


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------


def dataframe_literal_remap(
    dataframe: DataFrame,
    mapping: Dict[str, Dict[Any, Any]]
) -> DataFrame:
    """
    Remaps elements in a DataFrame based on a provided mapping.

    Args:
        dataframe (DataFrame): The DataFrame to remap.
        mapping (Dict[str, Dict[Any, Any]]): A dictionary where keys are column names (dimensions),
                                             and values are dictionaries mapping old elements to new elements.

    Returns:
        DataFrame: The updated DataFrame with elements remapped.
    """
    dataframe.replace({col: mapping[col] for col in mapping.keys() if col in dataframe.columns}, inplace=True)
    return dataframe


def dataframe_cube_remap(
    data_df: DataFrame,
    mapping_df: DataFrame,
    mapped_dimensions: Dict[str, str]
) -> DataFrame:
    """
    Map specified dimension columns in 'data_df' using 'mapping_df',
    optimized for memory efficiency by modifying dataframes in-place.

    Parameters
    ----------
    data_df : DataFrame
        The original source dataframe, whose columns we want to preserve except
        where we overwrite certain dimension values.
    mapping_df : DataFrame
        The dataframe containing the mapped values for certain columns.
    mapped_dimensions : dict
        A dictionary that specifies which columns in 'data_df' should be replaced
        by which columns in 'mapping_df'.

    Returns
    -------
    DataFrame
        A dataframe with the same columns (and order) as 'data_df',
        but with specified dimensions mapped from 'mapping_df'.
    """

    # 1) Compute shared dimensions, excluding those being remapped
    shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - set(mapped_dimensions.keys()))

    # 2) Perform an in-place left join on shared dimensions
    data_df = data_df.merge(mapping_df, how='left', on=shared_dimensions, suffixes=('', '_mapped'))

    # 3) Overwrite columns in data_df with their mapped versions, avoiding extra copies
    for data_col, map_col in mapped_dimensions.items():
        mapped_col_name = f"{map_col}_mapped" if map_col == data_col else map_col
        data_df[data_col] = data_df[mapped_col_name]
        del data_df[mapped_col_name]

    # 4) Retain only the original columns from data_df
    return data_df[data_df.columns.intersection(set(mapping_df.columns))]


def assign_mapping_dataframes(
    mapping_steps: List[Dict],
    shared_mapping_df: Optional[DataFrame] = None,
    shared_mapping_mdx: Optional[str] = None,
    shared_mapping_metadata_function: Optional[Callable[..., Any]] = None,
    mdx_function: Optional[Callable[..., DataFrame]] = None,
    tm1_service: Optional[Any] = None,
    **kwargs
) -> Dict[str, Optional[DataFrame] | List[Dict[str, Any]]]:
    """
    Assigns mapping DataFrames to mapping steps by either:
    - Using an existing 'mapping_df' in the step (if provided).
    - Generating a DataFrame from 'mapping_mdx' (if provided).
    - Falling back to 'shared_mapping_df' if neither is provided.

    Ensures that 'map_by_mdx' steps have at least one valid mapping source
    (either a step-specific 'mapping_df' or 'mapping_mdx', or a shared_mapping_df).

    Parameters:
    ----------
    mapping_steps : List[Dict]
        A list of mapping step dictionaries, each containing at least a 'method' key.
        - If 'method' is "map_by_mdx", it must include either 'mapping_df' or 'mapping_mdx',
          or else the shared_mapping_df must be provided.
        - If 'method' is "replace", no additional checks are applied.

    shared_mapping_df : Optional[DataFrame], default=None
        A shared DataFrame to be used if no 'mapping_df' or 'mapping_mdx' is provided.

    shared_mapping_mdx : Optional[str], default=None
        A shared MDX query string that will be converted into a DataFrame if no
        'mapping_df' or 'mapping_mdx' is provided.

    mdx_function : Optional[Callable[..., DataFrame]], default=None
        A function that takes an MDX query and returns a Pandas DataFrame.
        Used to convert 'mapping_mdx' queries into DataFrames.

    tm1_service : Optional[Any], default=None
        An optional service object used by 'mdx_function' if required.

    **kwargs : dict
        Additional keyword arguments that will be passed to 'mdx_function'.

    Returns:
    -------
    Dict[str, Any]
        A dictionary containing:
        - 'shared_mapping_df': The resolved shared mapping DataFrame.
        - 'mapping_data': The updated list of mapping steps, where each 'map_by_mdx' step
          has an assigned 'mapping_df' if necessary.

    Raises:
    ------
    ValueError
        If any 'map_by_mdx' step lacks both 'mapping_df' and 'mapping_mdx',
        AND 'shared_mapping_df' is also missing or empty.

    Example of the 'mapping_steps'::
    -------
        [
            {
                "method": "replace",
                "mapping": {
                    "dim1": {"source": "target"},
                    "dim2": {"source3": "target3", "source4": "target4"}
                }
            },
            {
                "method": "map_by_mdx",
                "mapping_mdx": "////valid mdx////",
                "mapping_metadata_function": mapping_metadata_function
                "mapping_df": mapping_dataframe
                "mapping_filter": {
                    "dim": "element",
                    "dim2": "element2"
                },
                "mapping_dims": {
                    "Organization Units": "Value"
                },
                "relabel_dimensions": false
            }
        ]
    """

    def create_dataframe(mdx: str, metadata_function: Optional[Callable[..., Any]] = None) -> DataFrame:
        """Helper function to convert MDX to a normalized DataFrame."""
        return mdx_to_normalized_dataframe(
            mdx_function=mdx_function,
            metadata_function=metadata_function,
            tm1_service=tm1_service,
            data_mdx=mdx,
            skip_zeros=True,
            skip_consolidated_cells=True,
            **kwargs
        )

    shared_mapping_df = shared_mapping_df or (
        create_dataframe(shared_mapping_mdx, shared_mapping_metadata_function) if shared_mapping_mdx else None
    )

    mapping_steps = [
        {
            **step,
            "mapping_df": step.get("mapping_df")
            or (create_dataframe(step["mapping_mdx"],
                                 step["mapping_metadata_function"]
                                 ) if "mapping_mdx" in step else None)
        }
        for step in mapping_steps
    ]

    missing_mdx_steps = [
        step for step in mapping_steps
        if step["method"] == "map_by_mdx" and not step.get("mapping_df")
    ]

    if missing_mdx_steps and (shared_mapping_df is None or shared_mapping_df.empty):
        raise ValueError(
            f"Missing mapping source for 'map_by_mdx' steps: {missing_mdx_steps}. "
            "Either provide 'mapping_mdx' or 'mapping_df' in these steps, or a valid 'shared_mapping_df'."
        )

    return {"shared_mapping_df": shared_mapping_df, "mapping_steps": mapping_steps}


def _apply_replace(data_df: DataFrame, mapping_step: Dict[str, Any], mapping_data: Dict[str, Any]) -> DataFrame:
    """
    Handle the 'replace' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The DataFrame to apply replacements on.
    mapping_step : Dict[str, Any]
        The dictionary containing information about the current mapping step.
    mapping_data : Dict[str, Any]
        The overall mapping data dictionary which can include shared or step-specific resources.

    Returns
    -------
    DataFrame
        The modified DataFrame after applying the literal remap.
    """
    _ = mapping_data
    return dataframe_literal_remap(
        dataframe=data_df,
        mapping=mapping_step["mapping"]
    )


def _apply_map_by_mdx(data_df: DataFrame, mapping_step: Dict[str, Any], mapping_data: Dict[str, Any]) -> DataFrame:
    """
    Handle the 'map_by_mdx' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame that will be remapped using the MDX approach.
    mapping_step : Dict[str, Any]
        The dictionary specifying how to map, which may contain 'mapping_filter',
        'mapping_mdx', 'mapping_dims', etc.
    mapping_data : Dict[str, Any]
        The overall mapping data dictionary. Used to fetch a shared DataFrame if
        a step-specific one is not provided.

    Returns
    -------
    DataFrame
        The modified DataFrame after applying the MDX-based remap.
    """
    step_uses_independent_mapping = (
        "mapping_df" in mapping_step and mapping_step["mapping_df"] is not None
    )

    mapping_df = (
        mapping_step["mapping_df"]
        if step_uses_independent_mapping
        else mapping_data["shared_mapping_df"]
    )

    if "mapping_filter" in mapping_step:
        mapping_df = dataframe_filter(
            dataframe=mapping_df,
            filter_condition=mapping_step["mapping_filter"],
            inplace=step_uses_independent_mapping
        )

    data_df = dataframe_cube_remap(
        data_df=data_df,
        mapping_df=mapping_df,
        mapped_dimensions=mapping_step["mapping_dims"]
    )

    if mapping_step.get("relabel_dimensions"):
        data_df = dataframe_relabel(
            dataframe=data_df,
            columns=mapping_step["mapping_dims"]
        )

    return data_df


def dataframe_execute_mappings(
    data_df: DataFrame,
    mapping_data: Dict[str, Optional[DataFrame] | List[Dict[str, Any]]]
) -> DataFrame:
    """
    Execute a series of mapping steps on data_df based on the instructions in
    mapping_data. Uses mutation for memory efficiency.
    Mapping filters mutate the step specific mapping dataframes, but don't mutate the shared one.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame to be transformed.
    mapping_data : Dict[str, Optional[DataFrame] | List[Dict[str, Any]]]
        A dictionary containing:
          - "mapping_steps": a list of dicts specifying each mapping step.
          - "shared_mapping_df": a shared DataFrame that may be used by multiple steps.

    Returns
    -------
    DataFrame
        The transformed DataFrame after all mapping steps have been applied.

    Example of the 'mapping_steps' inside mapping_data::
    -------
        [
            {
                "method": "replace",
                "mapping": {
                    "dim1": {"source": "target"},
                    "dim2": {"source3": "target3", "source4": "target4"}
                }
            },
            {
                "method": "map_by_mdx",
                "mapping_mdx": "////valid mdx////",
                "mapping_metadata_function": mapping_metadata_function
                "mapping_df": mapping_dataframe
                "mapping_filter": {
                    "dim": "element",
                    "dim2": "element2"
                },
                "mapping_dims": {
                    "Organization Units": "Value"
                },
                "relabel_dimensions": false
            }
        ]
    """
    method_handlers = {
        "replace": _apply_replace,
        "map_by_mdx": _apply_map_by_mdx,
    }
    for mapping_step in mapping_data["mapping_steps"]:
        method = mapping_step["method"]
        if method in method_handlers:
            data_df = method_handlers[method](data_df, mapping_step, mapping_data)
        else:
            raise ValueError(f"Unsupported mapping method: {method}")

    return data_df


def data_copy(
        tm1_service: Optional[Any],
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., DataFrame]] = None,
        target_cube_name: Optional[str] = None,
        mapping_steps: Optional[List[Dict]] = None,
        #target_metadata_function: Optional[Callable[..., DataFrame]] = None,
        shared_mapping_df: Optional[DataFrame] = None,
        shared_mapping_mdx: Optional[str] = None,
        shared_mapping_metadata_function: Optional[Callable[..., Any]] = None,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        clear_set_mdx_list: Optional[List[str]] = None,
        clear_target: Optional[bool] = False,
        async_write: bool = False,
        use_ti: bool = False,
        use_blob: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        **kwargs
) -> None:
    data_metadata = collect_metadata(
        tm1_service=tm1_service,
        mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs
    )

    if target_cube_name is None:
        target_cube_name = data_metadata.get_cube_name()

    data_metadata_function = lambda: data_metadata

    dataframe = mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        data_mdx_list=data_mdx_list,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        mdx_function=mdx_function,
    )

    dataframe = normalize_dataframe(
        dataframe=dataframe,
        metadata_function = data_metadata_function
    )

    mapping_data = assign_mapping_dataframes(
        mapping_steps=mapping_steps,
        shared_mapping_df=shared_mapping_df,
        shared_mapping_mdx=shared_mapping_mdx,
        shared_mapping_metadata_function=shared_mapping_metadata_function
    )

    dataframe = dataframe_execute_mappings(
        data_df=dataframe,
        mapping_data=mapping_data
    )

    dataframe = dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping
    )

    if value_function is not None:
        dataframe = dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    dataframe_to_cube_with_clear(
        tm1_service=tm1_service,
        dataframe=dataframe,
        clear_set_mdx_list=clear_set_mdx_list,
        clear_target=clear_target,
        async_write=async_write,
        use_ti=use_ti,
        increment=increment,
        use_blob=use_blob,
        sum_numeric_duplicates=sum_numeric_duplicates,
        cube_dims=data_metadata.get_cube_dims(),
        cube_name=target_cube_name
    )
