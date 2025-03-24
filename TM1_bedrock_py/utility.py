import os
import re
import functools
import time
import locale
from typing import Callable, List, Dict, Optional, Any, Union, Iterator

from mdxpy import MdxBuilder, MdxHierarchySet, Member
from sqlalchemy import create_engine, inspect
from pandas import DataFrame
from numpy import float64
from TM1_bedrock_py import exec_metrics_logger, basic_logger


# ------------------------------------------------------------------------------------------------------------
# Utility: Logging helper functions
# ------------------------------------------------------------------------------------------------------------


def execution_metrics_logger(func, *args, **kwargs):
    """Measures and logs the runtime of any function."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    exec_id = kwargs.get("_execution_id")

    if exec_id is None:
        exec_id = 0

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    exec_metrics_logger.debug(f"exec_time {execution_time:.2f} s", extra={
        "func": func.__name__,
        "fileName": os.path.basename(func.__code__.co_filename),
        "exec_id": f"exec_id {exec_id}"
    })

    return result


def log_exec_metrics(func):
    """Decorator to measure function execution time."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return execution_metrics_logger(func, *args, **kwargs)
    return wrapper


def set_logging_level(logging_level: str):
    basic_logger.setLevel(logging_level)
    exec_metrics_logger.setLevel(logging_level)


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------


def _get_cube_name_from_mdx(mdx_query: str) -> str:
    """
    Extracts the cube name from the FROM clause of an MDX query.

    Args:
        mdx_query (str): The MDX query string to parse.

    Returns:
        str: The name of the cube specified in the FROM clause.

    Raises:
        ValueError: If the MDX query does not contain a valid FROM clause.
    """
    from_part_match: Optional[re.Match[str]] = re.search(r"FROM\s*\[(.*?)]", mdx_query, re.IGNORECASE)
    if not from_part_match:
        raise ValueError("MDX query is missing the FROM clause.")
    return from_part_match.group(1).strip()


def _mdx_filter_to_dictionary(mdx_query: str) -> Dict[str, str]:
    """
    Parses the WHERE clause of an MDX query and extracts dimensions and their elements.

    Args:
        mdx_query (str): The MDX query string to parse.

    Returns:
        Dict[str, str]: A dictionary where keys are dimension names and values are element names.
    """
    where_match: Optional[re.Match[str]] = re.search(r'WHERE\s*\((.*?)\)', mdx_query, re.S)
    if not where_match:
        return {}

    where_content: str = where_match.group(1)
    mdx_dict: Dict[str, str] = {}

    hier_elements: List[tuple] = re.findall(r'\[(.*?)]\.?\[(.*?)]\.?\[(.*?)]', where_content)
    for dim, hier, elem in hier_elements:
        mdx_dict[dim] = elem

    remaining_content: str = re.sub(r'\[(.*?)]\.?\[(.*?)]\.?\[(.*?)]', '', where_content)
    dim_elements: List[tuple] = re.findall(r'\[(.*?)]\.?\[(.*?)]', remaining_content)

    for dim, elem in dim_elements:
        mdx_dict[dim] = elem

    return mdx_dict


def __transform_set_mdx_list_to_tm1py_clear_kwargs(mdx_expressions: List[str]) -> Dict[str, str]:
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


def __parse_unique_element_names_from_mdx(mdx_string: str) -> List[str]:
    """
    Extracts unique [X].[Y].[Z] patterns from an MDX string,
    where X, Y, and Z can contain spaces and special characters.

    Parameters:
        mdx_string (str): The input MDX query as a string.

    Returns:
        List[str]: A list of unique [X].[Y].[Z] style matches.
    """
    pattern = r'\[.*?\]\.\[.*?\]\.\[.*?\]'
    matches = re.findall(pattern, mdx_string)
    unique_matches = list(set(matches))
    return unique_matches


# ------------------------------------------------------------------------------------------------------------
# Utility: Cube metadata collection using input MDXs and/or other cubes
# ------------------------------------------------------------------------------------------------------------


class TM1CubeObjectMetadata:
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
    - `get_filter_dict`: Returns the filter dimensions and their elements in a {"dim":"elem", ...} dictionary format

    """

    # metadata parts, internal naming
    _QUERY_VAL = "query value"
    _QUERY_FILTER_DICT = "query filter dictionary"
    _CUBE_NAME = "cube name"
    _CUBE_DIMS = "dimensions"
    _CUBE_DIMS_LIST = "dimension list"
    _DIM_HIERS = "hierarchies"
    _DEFAULT_NAME = "default member name"
    _DEFAULT_TYPE = "default member type"
    _DIM_CHECK_DFS = "dimension check dataframes"

    def __init__(self) -> None:
        self._data: Dict[str, Union['TM1CubeObjectMetadata', Any]] = {}

    def __getitem__(self, item: str) -> Union['TM1CubeObjectMetadata', Any]:
        if item not in self._data:
            self._data[item] = TM1CubeObjectMetadata()
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
        return {k: v.to_dict() if isinstance(v, TM1CubeObjectMetadata) else v for k, v in self._data.items()}

    def to_list(self) -> list:
        return list(self._data.keys())

    def get_cube_name(self) -> str:
        return self[self._CUBE_NAME]

    def get_cube_dims(self) -> List[str]:
        return self[self._CUBE_DIMS_LIST]

    def get_filter_dict(self):
        return self[self._QUERY_FILTER_DICT]

    def get_dimension_check_dfs(self):
        return self[self._DIM_CHECK_DFS]

    @classmethod
    def _expand_query_metadata(cls, mdx: str, metadata: "TM1CubeObjectMetadata") -> None:
        """
        Extracts the filter dimensions and their elements in the mdx query (parts of the WHERE clause)

        Args:
            mdx (str): The MDX query string.
            metadata (TM1CubeObjectMetadata): The metadata object to update.

        Returns:
            TM1CubeObjectMetadata: The updated metadata object.
        """

        metadata[cls._QUERY_VAL] = mdx
        metadata[cls._QUERY_FILTER_DICT] = _mdx_filter_to_dictionary(mdx)

    @classmethod
    def _expand_base_cube_metadata(cls, tm1_service: Any, cube_name: str, metadata: "TM1CubeObjectMetadata") -> None:
        metadata[cls._CUBE_NAME] = cube_name
        metadata[cls._CUBE_DIMS_LIST] = tm1_service.cubes.get_dimension_names(cube_name)

    @classmethod
    def __collect_default(
            cls,
            tm1_service: Any,
            mdx: Optional[str] = None,
            cube_name: Optional[str] = None,
            retrieve_all_dimension_data: Optional[Callable[..., Any]] = None,
            retrieve_dimension_data: Optional[Callable[..., Any]] = None,
            collect_extended_cube_metadata: Optional[bool] = False,
            collect_dim_element_identifiers: Optional[bool] = False,
            **kwargs
    ) -> "TM1CubeObjectMetadata":
        """
        Collects important data about the mdx query and/or it's cube based on either an MDX query or a cube name.

        Args:
            tm1_service (Any): The TM1 service object used to interact with the cube.
            mdx (Optional[str]): The MDX query string.
            cube_name (Optional[str]): The name of the cube.
            retrieve_all_dimension_data (Optional[Callable]): A callable function to retrieve all dimension data.
            retrieve_dimension_data (Optional[Callable]): A callable function to handle metadata retrieval for dims.

        Returns:
            TM1CubeObjectMetadata: A structured metadata object containing information about the cube.

        Raises:
            ValueError: If neither an MDX query nor a cube name is provided.
        """

        metadata = TM1CubeObjectMetadata()

        if mdx:
            cube_name = _get_cube_name_from_mdx(mdx)
            cls._expand_query_metadata(mdx, metadata)
        cls._expand_base_cube_metadata(tm1_service=tm1_service, cube_name=cube_name, metadata=metadata)

        if not cube_name:
            raise ValueError("No MDX or cube name was specified.")

        if retrieve_all_dimension_data is None:
            retrieve_all_dimension_data = cls.__tm1_dimension_data_collector_for_cube

        if retrieve_dimension_data is None:
            retrieve_dimension_data = cls.__tm1_dimension_data_collector_default

        if collect_dim_element_identifiers:
            cls.__collect_element_check_dataframes(
                tm1_service=tm1_service, cube_dimensions=metadata.get_cube_dims(), metadata=metadata
            )

        if collect_extended_cube_metadata:
            retrieve_all_dimension_data(
                tm1_service=tm1_service,
                cube_dimensions=metadata.get_cube_dims(),
                metadata=metadata,
                retrieve_dimension_data=retrieve_dimension_data
            )

        return metadata

    @classmethod
    def collect(
            cls,
            metadata_function: Optional[Callable[..., DataFrame]] = None,
            **kwargs: Any
    ) -> "TM1CubeObjectMetadata":
        """
        Retrieves a Metadata object by executing the provided metadata function.

        Args:
            metadata_function (Optional[Callable]): A function to execute the MDX query and return a DataFrame.
                                               If None, the default function is used.
            **kwargs (Any): Additional keyword arguments passed to the MDX function.

        Returns:
            TM1CubeObjectMetadata: The Metadata object resulting from the metadata function call
        """
        if metadata_function is None:
            metadata_function = cls.__collect_default

        return metadata_function(**kwargs)

    @classmethod
    def __collect_element_check_dataframes(
            cls,
            tm1_service: Any,
            cube_dimensions: List[str],
            metadata: "TM1CubeObjectMetadata"
    ) -> None:
        metadata[cls._DIM_CHECK_DFS] = []
        for dimension in cube_dimensions:
            metadata[cls._DIM_CHECK_DFS].append(
                all_leaves_identifiers_to_dataframe(tm1_service=tm1_service, dimension_name=dimension)
            )

    @classmethod
    def __tm1_dimension_data_collector_for_cube(
            cls,
            tm1_service: Any,
            cube_dimensions: List[str],
            metadata: "TM1CubeObjectMetadata",
            retrieve_dimension_data: Callable[..., Any]
    ) -> None:
        """
        Default implementation to retrieve and update metadata for all dimensions of a cube.

        Args:
            tm1_service (Any): The TM1 service object.
            cube_dimensions (List[str]): A list of dimension names in the cube.
            metadata (TM1CubeObjectMetadata): The metadata object to update.
            retrieve_dimension_data (Callable): A function to retrieve and update metadata for each dimension.

        Returns:
            metadata (Metadata)
        """
        for dimension in cube_dimensions:
            dimension_hierarchies = tm1_service.hierarchies.get_all_names(dimension_name=dimension)
            retrieve_dimension_data(tm1_service, dimension, dimension_hierarchies, metadata)

    @classmethod
    def __tm1_dimension_data_collector_default(
            cls,
            tm1_service: Any,
            dimension: str,
            hierarchies: List[str],
            metadata: "TM1CubeObjectMetadata"
    ) -> None:
        """
        Default implementation to retrieve and collect metadata for a dimension and its hierarchies.

        Args:
            tm1_service (Any): The TM1 service object.
            dimension (str): The name of the dimension.
            hierarchies (List[str]): A list of hierarchies in the dimension.
            metadata (TM1CubeObjectMetadata): The metadata object to update.

        Returns:
            TM1CubeObjectMetadata: The updated metadata object.
        """
        for hierarchy in hierarchies:
            default_member = tm1_service.hierarchies.get(
                dimension_name=dimension, hierarchy_name=hierarchy
            ).default_member
            metadata[cls._CUBE_DIMS][dimension][cls._DIM_HIERS][hierarchy][cls._DEFAULT_NAME] = default_member

            default_member_type = tm1_service.elements.get(
                dimension_name=dimension, hierarchy_name=hierarchy, element_name=default_member
            ).element_type
            metadata[cls._CUBE_DIMS][dimension][cls._DIM_HIERS][hierarchy][cls._DEFAULT_TYPE] = default_member_type


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

    metadata = TM1CubeObjectMetadata.collect(metadata_function=metadata_function, cube_name=cube_name, **kwargs)
    dimensions_from_metadata = metadata.get_cube_dims()
    dimensions_from_dataframe = list(map(str, dataframe.keys()))
    dimensions_from_dataframe.remove("Value")

    return dimensions_from_metadata == dimensions_from_dataframe


def validate_dataframe_values_for_na(dataframe: DataFrame) -> bool:
    """
    Checks if the DataFrame rows contain NaN values in the "Value" column

    Args:
        dataframe: (DataFrame): The DataFrame to check for NaN values.
    Returns:
         boolean: True if the DataFrame does not contain NaN values.
                  False if it does.
    """

    return not dataframe["Value"].isna().values.any()


def validate_dataframe_no_duplicates(dataframe: DataFrame) -> bool:
    """
    Checks if the DataFrame rows contain duplicate values for validating the DataFrame.

    Args:
        dataframe: (DataFrame): The DataFrame to check for duplicate values.
    Returns:
         boolean: True if the DataFrame does not contain duplicate values.
                  False if it does.
    """
    return not dataframe.duplicated(keep=False).any()


def validate_dataframe_rows(dataframe: DataFrame) -> bool:
    """
    Checks if the DataFrame rows are valid. A row is valid if it does not contain duplicate or NaN values.

    Args:
        dataframe: (DataFrame): The DataFrame to check for duplicate and NaN values.
    Returns:
        boolean: True if the DataFrame does not contain duplicate or NaN values.
                 False if it does contain either.
    """
    return (validate_dataframe_values_for_na(dataframe=dataframe)
            and validate_dataframe_no_duplicates(dataframe=dataframe))


def validate_dataframe_for_cube_objects(
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


def validate_dataframe_transformations(
        source_dataframe: DataFrame,
        target_cube_name: str,
        source_dim_mapping: dict,
        related_dimensions: dict,
        target_dim_mapping: dict,
        **kwargs
) -> bool:

    source_dimensions = source_dataframe.columns()
    target_dimensions = TM1CubeObjectMetadata.collect(cube_name=target_cube_name, **kwargs).get_cube_dims()

    if source_dimensions == target_dimensions:
        return True
    else:
        return (__validate_dataframe_transformations_for_source(
                        source_dimensions=source_dimensions,
                        source_dim_mapping=source_dim_mapping,
                        related_dimensions=related_dimensions)
                and __validate_dataframe_transformations_for_target(
                        target_dimensions=target_dimensions,
                        target_dim_mapping=target_dim_mapping,
                        related_dimensions=related_dimensions)
                )


def __validate_dataframe_transformations_for_source(
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


def __validate_dataframe_transformations_for_target(
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
# Utility: MDX builder functions
# ------------------------------------------------------------------------------------------------------------


def build_mdx_from_cube_filter(
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

    metadata = TM1CubeObjectMetadata.collect(
        metadata_function=metadata_function, cube_name=cube_name, **kwargs
    )
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
# Utility: Additional helpers
# ------------------------------------------------------------------------------------------------------------


def force_float64_on_numeric_values(input_value: Any) -> float64 | str:
    """
    Convert string '12,34' → '12.34' and then parse as np.float64 if it is a numeric (optionally with decimal).
    Otherwise, return the original value unchanged.

    Args:
        input_value: any type of value to be converted if numerical

    Returns:
        the converted and cast numerical value or the string unchanged
    """

    if not isinstance(input_value, str):
        return float64(input_value)
    pattern = re.compile(r"^-?\d+(?:[.,]\d+)?$")
    if pattern.match(input_value):
        input_value = input_value.replace(',', '.')
    try:
        return float64(input_value)
    except ValueError:
        return input_value


def get_local_decimal_separator() -> str:
    locale.getlocale()
    return locale.localeconv()['decimal_point']


def get_local_regex_separator() -> str:
    """Detects the CSV separator based on the system's locale settings with cross-platform support."""
    try:
        locale.setlocale(locale.LC_ALL, "")
        decimal_sep = get_local_decimal_separator()

        csv_sep = ";" if decimal_sep == "," else ","

        return csv_sep
    except Exception as e:
        basic_logger.info(f"Warning: Unable to detect locale settings ({e}). Defaulting to comma (',').")
        return ","


def create_sql_engine(
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection_type: Optional[str] = None,
        connection_string: Optional[str] = None,
        host: Optional[str] = "localhost",
        port: Optional[str] = None,
        mssql_driver: Optional[str] = "ODBC+Driver+17+for+SQL+Server",
        sqlite_file_path: Optional[str] = None,
        oracle_sid: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs
) -> Any:
    connection_strings = {
        'mssql': f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver={mssql_driver}&TrustServerCertificate=yes",
        'sqlite': f"sqlite:///{sqlite_file_path}",
        'postgresql': f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
        'mysql': f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}",
        'mariadb': f"mariadb+mariadbconnector://{username}:{password}@{host}:{port}/{database}",
        'oracle': f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{oracle_sid}",
        'ibmdb2': f"ibm_db_sa://{username}:{password}@{host}:{port}/{database}",
        'sqlite_inmemory': "sqlite:///:memory:",
        'firebird': f"firebird+fdb://{username}:{password}@{host}:{port}/{database}",
    }
    if connection_type and not connection_string:
        connection_string = connection_strings.get(connection_type)
    return create_engine(connection_string)


def inspect_table(engine: Any, table_name: str) -> dict:
    return inspect(engine).get_columns(table_name)


@log_exec_metrics
def all_leaves_identifiers_to_dataframe(
        tm1_service: Any, dimension_name: [str], hierarchy_name: Optional[str] = None
) -> DataFrame:
    # caseandspaceinsensitiveset datastruct to dataframe
    if not hierarchy_name:
        hierarchy_name = dimension_name
    dataset = tm1_service.elements.get_all_leaf_element_identifiers(
        dimension_name=dimension_name, hierarchy_name=hierarchy_name
    )
    return DataFrame({dimension_name: list(dataset)})
