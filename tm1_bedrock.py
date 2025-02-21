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

    - `__getitem__`: Returns the value for the given key, creating a new nested `Metadata` if the key does not exist.
    - `__setitem__`: Sets the value for a specified key.
    - `__iter__`: Returns an iterator over the keys.
    - `__repr__` / `__str__`: Provides a string representation of the metadata keys.
    - `to_dict`: Recursively converts the metadata to a dictionary.
    - `to_list`: Returns a list of the top-level keys in the metadata.
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
    Collects metadata about a cube based on either an MDX query or a cube name.

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
    Extracts query-specific metadata from an MDX query.

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
    Collects the column labels from th DataFrame and compares them with the column labels collected from the Metadata based on a cube_name or metadata_function.
    Compares them as lists to check for matching label names and order.

    Args:
        dataframe: (DataFrame): The DataFrame to validate.
        cube_name: (srt): The name of the Cube.
        metadata_function: (Optional[Callable]): A function to collect metadata for validation.
                                                 If None, a default function is used.
        **kwargs: (Any): Additional keyword arguments passed to the function.
    Returns:
        boolean: True if the column label names and their order in the input DataFrame match the column labels of the Metadata.
                 False if the column labels or their order does not match.
    """

    if metadata_function is None:
        metadata_function = collect_metadata

    metadata = metadata_function(cube_name=cube_name, **kwargs)
    dimensions_from_metadata = metadata.get_cube_dims()
    dimensions_from_dataframe = list(map(str, dataframe.keys()))
    dimensions_from_dataframe.remove("Value")

    return dimensions_from_metadata == dimensions_from_dataframe


def validate_dataframe_not_NaN(
    dataframe: DataFrame
) -> bool:
    """
    Checks if the DataFrame rows contain NaN values for validating the DataFrame.

    Args:
        dataframe: (DataFrame): The DataFrame to check for NaN values.
    Returns:
         boolean: True if the DataFrame does not contain NaN values.
                  False if it does.
    """
    return not dataframe.isna().values.any()


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
    return validate_dataframe_not_NaN(dataframe) and validate_dataframe_no_duplicates(dataframe)


def validate_dataframe(
    dataframe: DataFrame,
    cube_name: str,
    metadata_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> bool:
    """
    Calls the validate_dataframe_rows() and validate_dataframe_columns() functions and returns only returns True if both conditions are met.

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
    return validate_dataframe_rows(dataframe) and validate_dataframe_columns(dataframe=dataframe, cube_name=cube_name, metadata_function=metadata_function, **kwargs)


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
    data_mdx: str,
    skip_zeros: bool = False,
    skip_consolidated_cells: bool = False,
    skip_rule_derived_cells: bool = False
) -> DataFrame:
    """
    Executes an MDX query using the default TM1 service function and returns a DataFrame.

    Args:
        tm1_service (TM1Service): An active TM1Service object for connecting to the TM1 server.
        data_mdx (str): The MDX query string to execute.
        skip_zeros (bool, optional): If True, cells with zero values will be excluded. Defaults to False.
        skip_consolidated_cells (bool, optional): If True, consolidated cells will be excluded. Defaults to False.
        skip_rule_derived_cells (bool, optional): If True, rule-derived cells will be excluded. Defaults to False.

    Returns:
        DataFrame: A DataFrame containing the result of the MDX query.
    """
    return tm1_service.cells.execute_mdx_dataframe(
        mdx=data_mdx,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells
    )


def normalize_dataframe(
    dataframe: DataFrame,
    metadata_function: Optional[Callable[..., Any]] = None,
    **kwargs: Any
) -> DataFrame:
    """
    Default implementation to normalize a DataFrame using metadata.

    Args:
        dataframe (DataFrame): The DataFrame to normalize.
        metadata_function (Optional[Callable]): A function to collect metadata for normalization.
                                                If None, a default function is used.
        **kwargs (Any): Additional keyword arguments for the metadata function.

    Returns:
        DataFrame: The normalized DataFrame.
    """
    if metadata_function is None:
        metadata_function = collect_metadata

    metadata = metadata_function(**kwargs)
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
    Retrieves and normalizes a DataFrame from an MDX query function.

    Args:
        mdx_function (Optional[Callable]): A function to retrieve a DataFrame from an MDX query.
                                           If None, a default function is used.
        normalize_function (Optional[Callable]): A function to normalize the retrieved DataFrame.
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
        dimension: dict,
        metadata_function: Optional[Callable[..., Any]] = None,
        **kwargs: Any
) -> str:

    if metadata_function is None:
        metadata_function = collect_metadata

    metadata = metadata_function(cube_name=cube_name, **kwargs)
    dataframe_dimensions = metadata.get_cube_dims()

    mdx_object = MdxBuilder.from_cube(cube_name)
    dim_keys = [key for key in dimension]

    for dim in dataframe_dimensions:
        if dim not in dim_keys:
            mdx_object.add_hierarchy_set_to_axis(1, MdxHierarchySet.all_leaves(dim))
        else:
            member_keys = [key for key in dimension[dim].keys()]
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
    mode: str = 'default'
) -> None:
    """
    Writes a DataFrame to a cube using the TM1 service.

    Args:
        tm1_service (TM1Service): An active TM1Service object for the TM1 server connection.
        dataframe (DataFrame): The DataFrame to write to the cube.
        cube_name (str): The name of the target cube.
        cube_dims (List[str]): A list of dimensions for the target cube.
        mode (str, optional): The mode for writing data ('default', 'ti', or 'blob'). Defaults to 'default'.
    """
    use_ti = mode == 'ti'
    use_blob = mode == 'blob'

    tm1_service.cells.write_dataframe(
        cube_name=cube_name,
        data=dataframe,
        dimensions=cube_dims,
        deactivate_transaction_log=True,
        reactivate_transaction_log=True,
        skip_non_updateable=True,
        use_ti=use_ti,
        use_blob=use_blob
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
    if write_function is not None:
        write_function(**kwargs)


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe transform utility functions
# ------------------------------------------------------------------------------------------------------------

# basic filter for 1 dimension-element
# basic addition for 1 dimension-element
# filter for nonzero then drop value column
def dataframe_filter(
        dataframe: DataFrame,
        filter_condition: dict
) -> DataFrame:
    """
    Filters DataFrame based on filter_condition. Only filters the DataFrame if at least one condition is met. If not, it returns an empty DataFrame.

    Args:
         dataframe: (DataFrame): The DataFrame to filter.
         filter_condition: (dict) Dimension:element key,value pairs for filtering the DataFrame.
    Returns:
        DataFrame: The updated DataFrame.
    """
    valid_columns = [col for col in filter_condition if col in dataframe.columns]

    if not valid_columns:
        return dataframe.iloc[0:0]

    condition = (dataframe[valid_columns] == pd.Series({col: filter_condition[col] for col in valid_columns})).all(axis=1)
    return dataframe.loc[condition]


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
    return dataframe.drop(column_list, axis=1)


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
        dataframe[list(new_columns)] = pd.DataFrame([new_columns], index=dataframe.index)

    return dataframe


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
    return dataframe_drop_column(dataframe=filtered_dataframe, column_list=column_list)


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
    return dataframe


def dataframe_relabel(
        dataframe: DataFrame,
        columns: dict
) -> None:
    """
    Relabels DataFrame column(s) if the original label is found in the DataFrame. If an original label is not found, then it is ignored.
    Args:
        dataframe: (DataFrame): The DataFrame to relabel.
        columns: (dict): The original and the new column labels as key-value pairs. The key stands for the original column label, the value for the new label.
    Return: None
    """
    dataframe.rename(columns=columns, inplace=True)


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
    for dimension, element_mapping in mapping.items():
        if dimension in dataframe.columns:
            dataframe[dimension].replace(element_mapping, inplace=True)
    return dataframe


def dataframe_settings_remap(
    main_dataframe: DataFrame,
    mapping_dataframe: DataFrame,
    target_mapping: Dict[str, Dict[str, Any]]
) -> DataFrame:
    """
    Remaps dimensions in the main DataFrame using values from the mapping DataFrame.

    Args:
        main_dataframe (DataFrame): The primary DataFrame with data to be remapped.
        mapping_dataframe (DataFrame): The DataFrame containing mapping values and keys.
        target_mapping (Dict[str, Dict[str, Any]]): A dictionary defining how to map dimensions.
            Format - {'Target Dimension': {'Mapping Dimension': 'Mapping Value'}}.

    Returns:
        DataFrame: The remapped DataFrame.

    Raises:
        ValueError: If any shared dimensions are missing in the main DataFrame.
    """
    shared_dimensions = list(set(main_dataframe.columns) & set(mapping_dataframe.columns))

    filtered_mapping = mapping_dataframe.copy()
    for target_dimension, mapping_info in target_mapping.items():
        for mapping_dimension, mapping_value in mapping_info.items():
            filtered_mapping = filtered_mapping[filtered_mapping[mapping_dimension] == mapping_value]

    missing_dims = [dim for dim in shared_dimensions if dim not in main_dataframe.columns]
    if missing_dims:
        raise ValueError(f"The following shared dimensions are missing in the main DataFrame: {missing_dims}")

    merged_df = main_dataframe.merge(filtered_mapping, on=shared_dimensions, how="left")

    for target_dimension, mapping_info in target_mapping.items():
        for mapping_dimension, _ in mapping_info.items():
            merged_df[target_dimension] = merged_df[mapping_dimension]

    merged_df = merged_df.drop(
        columns=[info for mapping in target_mapping.values() for info in mapping],
        errors="ignore"
    )

    return merged_df


def dataframe_cube_remap(
    data_df: DataFrame,
    mapping_df: DataFrame,
    mapped_dimensions: dict
) -> DataFrame:
    """
    Map specified dimension columns in 'data_df' using a 'mapping_df'.

    Steps:
        1) Identify shared dimensions (intersection of columns).
        2) Exclude from shared dimensions any column which appears in 'mapped_dimensions' keys,
           because we want to replace these columns, not join on them.
        3) Perform a left join on the remaining shared dimensions.
        4) For each (key, value) in 'mapped_dimensions', overwrite 'key' column
           in the joined dataframe with the data from the 'value' column in mapping_df.
        5) Return only the columns that were originally in 'data_df'.

    Parameters
    ----------
    data_df : pd.DataFrame
        The original source dataframe, whose columns we want to preserve except
        where we overwrite certain dimension values.
    mapping_df : pd.DataFrame
        The dataframe containing the mapped values for certain columns.
    mapped_dimensions : dict
        A dictionary that specifies which columns in 'data_df' should be replaced
        by which columns in 'mapping_df'. For example, {"orgunit": "orgunit_mapped"}.
        The key is the column name in data_df, the value is the column name in mapping_df.

    Returns
    -------
    pd.DataFrame
        A new dataframe with the same columns (and order) as 'data_df', but
        with specified dimensions mapped from 'mapping_df'.
    """

    # 1) Find columns in both data_df and mapping_df
    shared_dimensions = list(set(data_df.columns).intersection(set(mapping_df.columns)))

    # 2) Exclude columns that appear in the mapped_dimensions keys
    for dim in mapped_dimensions.keys():
        if dim in shared_dimensions:
            shared_dimensions.remove(dim)

    # 3) Perform a left join on these remaining shared columns
    #    We use suffixes=('', '_mapped') to avoid collisions
    joined_df = data_df.merge(
        mapping_df,
        how='left',
        on=shared_dimensions,
        suffixes=('', '_mapped')
    )

    # 4) Overwrite columns in data_df with the mapped columns
    for data_col, map_col in mapped_dimensions.items():
        # If the mapped column name is the same, it will appear in joined as 'map_col_mapped'
        # if it was not used in the join. Otherwise, if the name is different, it should appear
        # exactly as 'map_col'. Use whichever logic fits your data best.
        #
        # Below we handle the case if the mapped column is the same name as the key:
        map_col_in_joined = map_col
        if map_col == data_col:
            map_col_in_joined = f"{map_col}_mapped"

        joined_df[data_col] = joined_df[map_col_in_joined]

    # 5) Retain only the original columns from data_df
    mapped_df = joined_df[data_df.columns]

    return mapped_df


"""
def transform_dataframe_to_target_dataframe(
    tm1_service,
    data_df,
    target_cube,
    source_dimension_mapping_for_copy,
    target_dimension_mapping_for_copy,
    dimension_mapping_for_copy
):
    """"""
    Transforms the input DataFrame to match the dimensionality of the target cube.

    Args:
        tm1_service (TM1Service): Active TM1py service instance.
        data_df (DataFrame): DataFrame containing source cube's data.
        target_cube (str): Name of the target cube.
        source_dimension_mapping_for_copy (dict, optional): Mapping for extra source dimensions.
        target_dimension_mapping_for_copy (dict, optional): Mapping for extra target dimensions.
        dimension_mapping_for_copy (dict, optional): Mapping for dimensions unique to source and target cubes.

    Returns:
        DataFrame: Transformed DataFrame in the dimensionality of the target cube.

    Raises:
        ValueError: If required mappings or defaults are missing or invalid.
    """"""

    source_dimensions = list(data_df.columns)
    target_dimensions = tm1_service.cubes.get_dimension_names(target_cube)

    # Handle cases where dimensions are unequal
    # Case 1: Dimensions are equal
    if set(source_dimensions) == set(target_dimensions):
        return data_df

    # Case 2: Source cube has extra dimensions
    for dim in set(source_dimensions) - set(target_dimensions):
        if source_dimension_mapping_for_copy and dim in source_dimension_mapping_for_copy:
            data_df = data_df[data_df[dim] == source_dimension_mapping_for_copy[dim]]
            data_df = data_df.drop(columns=[dim])
        else:
            # default_element = get_hierarchy_default_element(tm1_service, dim, dim)
            default_element = ""
            if not default_element:
                raise ValueError(f"No mapping or default element for source dimension '{dim}'.")
            data_df = data_df[data_df[dim] == default_element]
            data_df = data_df.drop(columns=[dim])

    # Case 3: Target cube has extra dimensions
    for dim in set(target_dimensions) - set(source_dimensions):
        if target_dimension_mapping_for_copy and dim in target_dimension_mapping_for_copy:
            data_df[dim] = target_dimension_mapping_for_copy[dim]
        else:
            # default_element = get_hierarchy_default_element(tm1_service, dim, dim)
            # element_writable = element_is_writable(tm1_service, dim, dim, default_element)
            default_element = ""
            element_writable = False
            if not default_element or not element_writable:
                raise ValueError(f"No mapping, writable element, or default element for target dimension '{dim}'.")
            data_df[dim] = default_element

    # Case 4: Handle dimension mapping using dimension_mapping_for_copy
    if dimension_mapping_for_copy:
        for src_dim, tgt_dim in dimension_mapping_for_copy.items():
            if src_dim in data_df.columns and tgt_dim in target_dimensions:
                data_df[tgt_dim] = data_df[src_dim]
                data_df.drop(columns=[src_dim], inplace=True)

    # Ensure DataFrame has all target dimensions in the correct order
    missing_dims = set(target_dimensions) - set(data_df.columns)
    if missing_dims:
        raise ValueError(f"The transformed DataFrame is missing target dimensions: {missing_dims}")

    # Reorder DataFrame to match target cube dimensions
    data_df = data_df[target_dimensions + list(set(data_df.columns) - set(target_dimensions))]
    return data_df
"""
