import os
import re
import functools
import threading
import time
import locale
import itertools
from typing import Iterable, Callable, List, Dict, Optional, Any, Union, Iterator, Tuple, Literal

import pandas
from mdxpy import MdxBuilder, MdxHierarchySet, Member
from sqlalchemy import create_engine, inspect
from pandas import DataFrame
from numpy import float64
from datetime import datetime

from TM1_bedrock_py import exec_metrics_logger, basic_logger, benchmark_metrics_logger


# ------------------------------------------------------------------------------------------------------------
# Utility: Logging helper functions
# ------------------------------------------------------------------------------------------------------------

def generate_valid_file_path(output_dir: str, filename: str):
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    return filepath


def dataframe_verbose_logger(
        dataframe: DataFrame,
        step_number: str = None,
        verbose_logging_output_dir="logs/dataframe_logs",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        **_kwargs
):
    if verbose_logging_mode and dataframe is not None:
        if verbose_logging_mode == "file":
            thread_id = threading.get_ident()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

            filename = f"{step_number}_{thread_id}_{timestamp}.csv"
            filepath = generate_valid_file_path(verbose_logging_output_dir, filename)

            dataframe.to_csv(path_or_buf=filepath, index=False)
            basic_logger.debug(f"DataFrame logged to {filepath}")

        elif verbose_logging_mode == "print_console":
            num_rows_to_log = 5
            basic_logger.debug(f"First {num_rows_to_log} rows of DataFrame:\n{dataframe.head(num_rows_to_log)}")


def execution_metrics_logger(logger, func, *args, **kwargs):
    """Measures and logs the runtime of any function."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    exec_id = kwargs.get("_execution_id")

    if exec_id is None:
        exec_id = 0

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    logger.debug(f"exec_time {execution_time:.2f} s", extra={
        "func": func.__name__,
        "fileName": os.path.basename(func.__code__.co_filename),
        "exec_id": f"exec_id {exec_id}"
    })

    return result


async def async_execution_metrics_logger(logger, func, *args, **kwargs):
    """Measures and logs the runtime of any function."""
    start_time = time.perf_counter()
    result = await func(*args, **kwargs)
    exec_id = kwargs.get("_execution_id")

    if exec_id is None:
        exec_id = 0

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    logger.debug(f"exec_time {execution_time:.2f} s", extra={
        "func": func.__name__,
        "fileName": os.path.basename(func.__code__.co_filename),
        "exec_id": f"exec_id {exec_id}"
    })

    return result


def log_exec_metrics(func):
    """Decorator to measure function execution time."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return execution_metrics_logger(exec_metrics_logger, func, *args, **kwargs)
    return wrapper


def log_async_exec_metrics(func):
    """Decorator to measure function execution time."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await execution_metrics_logger(exec_metrics_logger, func, *args, **kwargs)
    return wrapper


def log_benchmark_metrics(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return execution_metrics_logger(benchmark_metrics_logger, func, *args, **kwargs)

    return wrapper


def log_async_benchmark_metrics(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await async_execution_metrics_logger(benchmark_metrics_logger, func, *args, **kwargs)

    return wrapper


def set_logging_level(logging_level: str):
    basic_logger.setLevel(logging_level)
    exec_metrics_logger.setLevel(logging_level)
    benchmark_metrics_logger.setLevel(logging_level)


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------


def get_cube_name_from_mdx(mdx_query: str) -> str:
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


def mdx_filter_to_dictionary(mdx_query: str) -> Dict[str, str]:
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


def get_kwargs_dict_from_set_mdx_list(mdx_expressions: List[str]) -> Dict[str, str]:
    """
    Generate a dictionary of kwargs from a list of MDX expressions.

    Args:
        mdx_expressions (List[str]): A list of MDX expressions.

    Returns:
        Dict[str, str]: A dictionary where keys are dimension names (in lowercase, spaces removed)
            and values are the MDX expressions.
    """
    if not isinstance(mdx_expressions, list):
        raise TypeError("Expected mdx_sets to be a list")
    if len(mdx_expressions) == 0:
        raise ValueError("Set mdx list cannot be empty")

    regex = r".*?(?<!\.)\[\s*([\w\s]+?)\s*\].*?"
    kwargs_dict = {}
    for mdx in mdx_expressions:
        match = re.search(regex, mdx)
        if match:
            key = match.group(1).lower().replace(" ", "")
            kwargs_dict[key] = mdx
        else:
            raise ValueError("The set mdx " + mdx + " is invalid")

    if len(mdx_expressions) == len(kwargs_dict):
        return kwargs_dict
    else:
        raise ValueError("Duplicate set mdx for a dimension")


def get_dimensions_from_set_mdx_list(mdx_sets: List[str]) -> List[str]:
    """
    Extracts the first dimension name found in each string of a list of MDX set strings.

    Note: This function finds only the first dimension pattern '{[Dimension]' in each string.
    It does not parse full MDX sets or guarantee uniqueness based on the strict implementation,
    though the naming extraction attempts to isolate the dimension.

    MDX members are expected in formats like:
    {[Dimension].[Hierarchy].[Element]}
    or
    {[Dimension].[Element]}

    Handles whitespace:
    - Around braces {}, dots . and brackets []
    - Inside the first dimension bracket, e.g., { [  Dimension Name  ] }

    Args:
        mdx_sets: A list of strings, where each string might represent an MDX set.
                  Can be None or contain None elements.

    Returns:
        A list of dimension names found (one per matching input string), ordered
        according to the input list. Returns an empty list if no valid MDX patterns are
        found or the input list is empty/None.

    Raises:
        TypeError: If mdx_sets is not a list or if any element
                   (that is not None) is not a string.
    """
    if not isinstance(mdx_sets, list):
        raise TypeError("Expected mdx_sets to be a list")
    if len(mdx_sets) == 0:
        raise ValueError("Set mdx list cannot be empty")

    pattern = r".*?(?<!\.)\[\s*([\w\s]+?)\s*\].*?"
    ordered_dimension_names = []

    for mdx_set_string in mdx_sets:
        if mdx_set_string is None:
            raise ValueError("NoneType set mdx encountered")
        if not isinstance(mdx_set_string, str):
            raise TypeError(f"Expected elements of mdx_sets to be strings, but found {type(mdx_set_string).__name__}")

        match = re.search(pattern, mdx_set_string)
        if match:
            full_match = match.group(1).strip()
            dimension_part = full_match.split('.', 1)[0].strip()
            if dimension_part:
                ordered_dimension_names.append(dimension_part)
        else:
            raise ValueError("The mdx " + mdx_set_string + " is invalid")

    if len(ordered_dimension_names) == len(set(ordered_dimension_names)):
        return ordered_dimension_names
    else:
        raise ValueError("Duplicate set mdxs for a dimension")


def generate_cartesian_product(list_of_lists: Optional[List[Optional[Iterable[Any]]]]) -> List[Tuple[Any, ...]]:
    """
    Generates the Cartesian product of a list of lists (or other iterables).

    Takes a list containing multiple iterables (like lists) and returns a list of tuples,
    where each tuple is a unique combination formed by taking one element
    from each input iterable, in the order they were provided.

    Args:
        list_of_lists: A list where each element is itself an iterable (e.g., list).
                       Can be None or contain None elements (treated as empty iterables).
                       Example: [["a","b"], ["1","2"], ["y","z"]]

    Returns:
        A list of tuples representing the Cartesian product. Returns an empty list
        if the input list is None, empty, or if any of the inner iterables result
        in an empty product (e.g., an inner list is empty).

    Raises:
        TypeError: If list_of_lists is not a list, or if any element within
                   list_of_lists is not None and not iterable.
    """
    if not list_of_lists:
        return []

    if not isinstance(list_of_lists, list):
        raise TypeError(f"Input must be a list, but got {type(list_of_lists).__name__}")

    product_iterator = itertools.product(*list_of_lists)
    result = list(product_iterator)

    return result


def generate_element_lists_from_set_mdx_list(
        tm1_service: Optional[Any], set_mdx_list: Optional[List[Optional[str]]]) -> List[List[str]]:
    """
    Executes multiple MDX set queries and extracts element names.

    For each MDX query string in the input list, this function calls the
    `tm1_service.elements.execute_set_mdx` method. It then processes the
    results, expecting a specific nested structure, to extract a list of
    element names for each query.

    Expected structure from tm1_service.elements.execute_set_mdx:
    A list where each item corresponds to an element in the MDX result set.
    Each item itself is expected to be a list containing at least one dictionary,
    and that first dictionary must have a 'Name' key.
    Example return for one query: [ [{'Name': 'ElementA'}], [{'Name': 'ElementB'}] ]

    Args:
        tm1_service: An object representing the TM1 service connection,
                     expected to have an `elements.execute_set_mdx` method.
                     Can be None.
        set_mdx_list: A list of strings, where each string is an MDX query.
                      Can be None or contain None elements (which are skipped).

    Returns:
        A list of lists of strings. Each inner list contains the element names
        extracted from the result of the corresponding MDX query. Returns an
        empty list if tm1_service or set_mdx_list is None or empty.

    Raises:
        ValueError: If tm1_service is None.
        TypeError: If set_mdx_list is not a list, or if any non-None element
                   within set_mdx_list is not a string.
        AttributeError: If tm1_service does not have the expected
                        `elements.execute_set_mdx` structure.
        IndexError: If the data returned by `execute_set_mdx` does not conform
                    to the expected nested list structure (e.g., inner list empty).
        KeyError: If the dictionary within the nested structure does not contain
                  the 'Name' key.
    """
    if tm1_service is None:
        raise ValueError("tm1_service cannot be None")

    if not set_mdx_list:
        return []

    if not isinstance(set_mdx_list, list):
        raise TypeError(f"Expected set_mdx_list to be a list, but got {type(set_mdx_list).__name__}")

    list_of_raw_results = []
    for query in set_mdx_list:
        if not isinstance(query, str):
            raise TypeError(f"Expected elements of set_mdx_list to be strings, but found {type(query).__name__}")

        raw_result = tm1_service.elements.execute_set_mdx(
            mdx=query,
            element_properties=None,
            member_properties=None,
            parent_properties=None
        )
        list_of_raw_results.append(raw_result)

    final_result = []
    for middle_list in list_of_raw_results:
        element_names = [inner_list[0]['Name'] for inner_list in middle_list]
        final_result.append(element_names)

    return final_result


def add_non_empty_to_mdx(mdx_string: str) -> str:
    """
    Adds 'NON EMPTY' before each axis definition ({...} ON ...) in an MDX
    SELECT statement, if it's not already present.

    Handles variations in whitespace (including newlines), axis identifiers
    (COLUMNS, ROWS, 0, 1, AXIS(n)), and case-insensitivity of keywords.
    It operates only between the main SELECT and FROM clauses.

    Args:
        mdx_string: A string containing a potentially valid MDX VIEW query.

    Returns:
        A string with ' NON EMPTY ' prepended to each axis set definition
        ({<set>} ON <axis>) that doesn't already have it,
        or the original string if the structure isn't a recognizable
        SELECT...FROM query or if modifications are not needed.
    """
    select_match = re.search(r'\bSELECT\b', mdx_string, re.IGNORECASE)
    if not select_match:
        return mdx_string

    from_match = re.search(r'\bFROM\b', mdx_string[select_match.end():], re.IGNORECASE)
    if not from_match:
        return mdx_string

    select_start_index = select_match.start()
    select_end_index = select_match.end()
    from_start_index = select_match.end() + from_match.start()

    prefix = mdx_string[:select_start_index]
    select_keyword_part = mdx_string[select_start_index:select_end_index]
    axes_definition_part = mdx_string[select_end_index:from_start_index]
    suffix = mdx_string[from_start_index:]

    axis_pattern = re.compile(
        r"""
        # Optional Capture Group 1: Potential 'NON EMPTY' prefix
        # Allows variable whitespace between NON, EMPTY, and the axis definition
        ( \b NON \s+ EMPTY \s+ )?

        # Capture Group 2: The core axis definition ({...} ON <axis>)
        (                 # Start Capture Group 2
            # Start of the Set Definition
            \{            # Literal opening brace of the set
            [\s\S]*?      # The content of the set (any char, non-greedy)
            \s*           # Optional whitespace before closing brace
            \}            # Literal closing brace of the set

            # Separator between Set and Axis Specifier
            \s+           # Required whitespace after the set (use + for robustness)
            \b ON \b      # The keyword ON (case-insensitive, whole word)
            \s+           # Required whitespace after ON (use + for robustness)

            # Axis Specifier (case-insensitive for names)
            (?:           # Start Non-Capturing Group for axis alternatives
                COLUMNS|ROWS|PAGES|SECTIONS|CHAPTERS # Common axis names
                |         # OR
                AXIS \s* \( \s* \d+ \s* \)            # AXIS(n) function call
                |         # OR
                \d+       # Axis ordinal number (0, 1, 2...)
            )             # End Non-Capturing Group
            \b            # Word boundary
        )                 # End Capture Group 2
        """,
        re.IGNORECASE | re.VERBOSE
    )

    def replacer(match):
        non_empty_group = match.group(1)
        axis_def_group = match.group(2)

        if non_empty_group:
            return match.group(0)
        else:
            return " NON EMPTY " + axis_def_group

    modified_axes_part = axis_pattern.sub(replacer, axes_definition_part)
    final_mdx = prefix + select_keyword_part + modified_axes_part + suffix
    return final_mdx


def extract_mdx_components(mdx: str) -> List[str]:
    """
    Extract all axis and filter items from MDX.
    - Splits axes on ON (outside brackets) and ',' (outside brackets)
    - Splits each axis item on '*'
    - WHERE clause is optional
    """

    cleaned_mdx = re.sub(r"NON\s*EMPTY", "", mdx)

    def extract_select_part(input_mdx: str):
        mdx_clean = input_mdx

        # -------------------
        # 1. Extract everything between SELECT and FROM
        # -------------------
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM", mdx_clean, re.IGNORECASE | re.DOTALL)
        if not select_match:
            raise ValueError("No SELECT ... FROM clause found")
        select_part = select_match.group(1)

        axis_item_pattern = re.compile(
            r"(?:\s*(?:\{.*?\}|\[.*?\]|[^\[\]{},])+?\s*)(?=(?:ON\b|,|$))",
            re.IGNORECASE | re.DOTALL
        )
        axes_raw = axis_item_pattern.findall(select_part)

        axes = []
        for item in axes_raw:
            # Remove trailing whitespace
            item_clean = item.strip()
            if item_clean.upper() not in ("ON COLUMNS", "ON ROWS") and item_clean.upper()[:3] != "ON ":
                axes.extend([p.strip() for p in item_clean.split('*') if p.strip()])
        return axes

    def extract_where_part(input_mdx: str):
        where_match = re.search(r"WHERE\s*\(\s*(.*?)\s*\)", input_mdx, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return []

        where_content = where_match.group(1)
        item_pattern = re.compile(
            r"""
            \s*                             # optional leading whitespace
            (                               # capture group
                (?:                         # non-capturing group
                    \[[^\]]*\]              # match [ ... ] bracketed part
                    |                       # OR
                    [^\[\],]                # any char except brackets or comma
                )+                          # repeat one or more times
            )
            \s*                             # optional trailing whitespace
            (?:,|$)                         # stop at comma or end (comma not consumed)
            """,
            re.VERBOSE | re.DOTALL
        )

        members = [m.group(1).strip() for m in item_pattern.finditer(where_content)]
        return members

    set_mdx_list = extract_select_part(cleaned_mdx) + extract_where_part(cleaned_mdx)
    set_mdx_list = ["{" + re.sub(r"\s+", "", item) + "}" for item in set_mdx_list]
    return set_mdx_list


def normalize_string(input_string: str) -> str:
    """Normalize a  string value for comparison (case- and space-insensitive)."""
    return re.sub(r'\s+', '', input_string.strip().lower())


def normalize_structure_strings(d: Any) -> Any:
    """Normalize a dictionary for comparison (case- and space-insensitive).
    Converts all strings, leaves everything else untouched"""
    if isinstance(d, dict):
        return {normalize_string(k): normalize_structure_strings(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [normalize_structure_strings(i) for i in d]
    elif isinstance(d, str):
        return normalize_string(d)
    elif isinstance(d, DataFrame):
        return normalize_dataframe_strings(d)
    return d


def normalize_dataframe_strings(dataframe: DataFrame) -> None:
    """Normalize a dataframe, including columns, string values, and object type columns with strings."""
    if not getattr(dataframe, "normalized", False):
        dataframe.rename(columns=lambda c: normalize_string(str(c)), inplace=True)
        for col in dataframe.select_dtypes(include=['object', 'string']):
            dataframe[col] = dataframe[col].map(lambda x: normalize_string(x) if isinstance(x, str) else x)
        dataframe.normalized = True
        basic_logger.debug("Dataframe object " + str(id(dataframe)) + " was normalized.")
    else:
        basic_logger.debug("Dataframe object " + str(id(dataframe)) + " already normalized.")

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
    _MEASURE_ELEMENT_TYPES = "measure element types"
    _SOURCE_CUBE_DIMS_LIST = "source dimension list"

    def __init__(self) -> None:
        self._data: Dict[str, Union['TM1CubeObjectMetadata', Any]] = {}

    def __getitem__(self, item: str) -> Any:
        if item not in self._data:
            self._data[item] = None
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

    def get_measure_element_types(self) -> Dict[str, str]:
        return self[self._MEASURE_ELEMENT_TYPES]

    def get_source_cube_dims(self) -> List[str]:
        return self[self._SOURCE_CUBE_DIMS_LIST]

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
        metadata[cls._CUBE_NAME] = get_cube_name_from_mdx(mdx)
        metadata[cls._QUERY_VAL] = mdx
        metadata[cls._QUERY_FILTER_DICT] = mdx_filter_to_dictionary(mdx)

    @classmethod
    def _expand_base_cube_metadata(cls, tm1_service: Any, cube_name: str, metadata: "TM1CubeObjectMetadata") -> None:
        metadata[cls._CUBE_NAME] = cube_name
        metadata[cls._CUBE_DIMS_LIST] = tm1_service.cubes.get_dimension_names(cube_name)

    @classmethod
    def _expand_source_cube_metadata(cls, tm1_service: Any, cube_name: str, metadata: "TM1CubeObjectMetadata") -> None:
        metadata[cls._SOURCE_CUBE_DIMS_LIST] = tm1_service.cubes.get_dimension_names(cube_name)

    @classmethod
    def __collect_default(
            cls,
            tm1_service: Optional[Any] = None,
            mdx: Optional[str] = None,
            cube_name: Optional[str] = None,
            collect_base_cube_metadata: Optional[bool] = True,
            collect_dim_element_identifiers: Optional[bool] = False,
            collect_measure_types: Optional[bool] = False,
            collect_source_cube_metadata: Optional[bool] = False,
            **_kwargs
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
            cls._expand_query_metadata(mdx, metadata)
            cube_name = cls.get_cube_name(metadata)

        if not cube_name:
            basic_logger.error("You need to have either an MDX or a cube name specified.")

        if collect_source_cube_metadata:
            cls._expand_source_cube_metadata(tm1_service=tm1_service, cube_name=cube_name, metadata=metadata)

        if collect_base_cube_metadata:
            cls._expand_base_cube_metadata(tm1_service=tm1_service, cube_name=cube_name, metadata=metadata)

        if collect_dim_element_identifiers:
            cls.__collect_element_check_dataframes(
                tm1_service=tm1_service, cube_dimensions=metadata.get_cube_dims(), metadata=metadata
            )

        if collect_measure_types:
            cube_dims = metadata.get_cube_dims()
            if cube_dims:
                measure_dimension = cube_dims[-1]
                metadata[cls._MEASURE_ELEMENT_TYPES] = tm1_service.elements.get_element_types(
                    dimension_name=measure_dimension, hierarchy_name=measure_dimension
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

def cast_coordinates_to_str(cube_dims: list, dataframe: pandas.DataFrame):
    """
        Convert all dimension (coordinate) columns in the given DataFrame to string type
        for TM1py compatibility. The 'Value' column, if present, is left unchanged.

        Args:
            cube_dims: List of cube dimension (coordinate) column names.
            dataframe: DataFrame whose dimension columns will be cast to string.

        Returns:
             The same DataFrame instance with dimension columns converted to string type.
    """
    basic_logger.info("Converting dimension columns to string type for consistency.")
    for dim_col in cube_dims:
        if dim_col in dataframe.columns:
            dataframe[dim_col] = dataframe[dim_col].astype(str)


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
    """ So far tested for mssql and postgresql connections. Expected to be extended in the future. """
    connection_strings = {
        'mssql': f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver={mssql_driver}&TrustServerCertificate=yes&fast_executemany=true",
        'postgresql': f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
        #'sqlite': f"sqlite:///{sqlite_file_path}",
        #'mysql': f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}",
        #'mariadb': f"mariadb+mariadbconnector://{username}:{password}@{host}:{port}/{database}",
        #'oracle': f"oracle+cx_oracle://{username}:{password}@{host}:{port}/{oracle_sid}",
        #'ibmdb2': f"ibm_db_sa://{username}:{password}@{host}:{port}/{database}",
        #'sqlite_inmemory': "sqlite:///:memory:",
        #'firebird': f"firebird+fdb://{username}:{password}@{host}:{port}/{database}",
    }
    if connection_type and not connection_string:
        connection_string = connection_strings.get(connection_type)
        if 'mssql' in connection_string:
            return create_engine(connection_string, fast_executemany=True)
    return create_engine(connection_string)


def inspect_table(engine: Any, table_name: str, schema: Optional[str]=None) -> dict:
    return inspect(engine).get_columns(table_name=table_name, schema=schema)


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
