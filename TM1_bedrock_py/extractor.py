from typing import Callable, List, Dict, Optional, Any

import pandas as pd
from TM1py import TM1Service
from pandas import DataFrame, read_sql_table, read_sql_query, concat
from typing import Sequence, Hashable, Mapping, Iterable
from pandas._typing import UsecolsArgType

from TM1_bedrock_py import utility, transformer, basic_logger


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def tm1_mdx_to_dataframe(
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
        mdx_function = __tm1_mdx_to_dataframe_default

    return mdx_function(**kwargs)


def __tm1_mdx_to_dataframe_default(
        tm1_service: TM1Service,
        data_mdx: Optional[str] = None,
        data_mdx_list:  Optional[list[str]] = None,
        skip_zeros: bool = False,
        skip_consolidated_cells: bool = False,
        skip_rule_derived_cells: bool = False,
        **kwargs
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
            use_iterative_json=True,
            use_blob=True,
            decimal=utility.get_local_decimal_separator()
        )
    elif data_mdx:
        return tm1_service.cells.execute_mdx_dataframe(
            mdx=data_mdx,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True,
            use_blob=True,
            decimal=utility.get_local_decimal_separator()
        )

    msg = "Either data_mdx or data_mdx_list has to be specified."
    basic_logger.error(msg)
    raise ValueError(msg)


# ------------------------------------------------------------------------------------------------------------
# mapping handler extractors
# ------------------------------------------------------------------------------------------------------------


def _handle_mapping_df(
    step: Dict[str, Any],
    **_kwargs
) -> DataFrame:
    return step["mapping_df"]


def _handle_mapping_mdx(
    step: Dict[str, Any],
    mdx_function: Optional[Callable[..., DataFrame]] = None,
    tm1_service: Optional[Any] = None,
    **kwargs
) -> DataFrame:
    """Execute MDX and augment the resulting DataFrame with metadata."""
    mdx = step["mapping_mdx"]
    step_specific_tm1_service = step.get("tm1_service")
    if not step_specific_tm1_service:
        step_specific_tm1_service = tm1_service
    dataframe = tm1_mdx_to_dataframe(
        mdx_function=mdx_function,
        tm1_service=step_specific_tm1_service,
        data_mdx=mdx,
        skip_zeros=True,
        skip_consolidated_cells=True,
        **kwargs
    )
    metadata_object = utility.TM1CubeObjectMetadata.collect(
        metadata_function=step.get("mapping_metadata_function"),
        tm1_service=step_specific_tm1_service,
        mdx=mdx,
        **kwargs
    )
    filter_dict = metadata_object.get_filter_dict()

    transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value=filter_dict)
    transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe)

    return dataframe


def _handle_mapping_sql_query(
        step: Dict[str, Any],
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable] = None,
        **kwargs
) -> DataFrame:
    table_name = step.get("sql_table_name")
    schema = step.get("sql_schema")
    sql_query = step.get("mapping_sql_query")
    dataframe = sql_to_dataframe(
        sql_function=sql_function, engine=sql_engine,
        sql_query=sql_query, schema=schema, table_name=table_name,
        **kwargs
    )
    columns_to_keep = step.get("sql_columns_to_keep")
    column_mapping = step.get("sql_column_mapping")
    value_column = step.get("sql_value_column")
    drop_other = step.get("sql_drop_other_cols")

    transformer.normalize_table_source_dataframe(
        dataframe=dataframe,
        columns_to_keep=columns_to_keep, column_mapping=column_mapping, value_column_name=value_column,
        drop_other_columns=drop_other
    )
    return dataframe


def _handle_mapping_csv(
        step: Dict[str, Any],
        **_kwargs
) -> None:
    return None


MAPPING_HANDLERS = {
    "mapping_df": _handle_mapping_df,
    "mapping_mdx": _handle_mapping_mdx,
    "mapping_sql_query": _handle_mapping_sql_query,
    "mapping_csv": _handle_mapping_csv
}


@utility.log_exec_metrics
def generate_dataframe_for_mapping_info(
        mapping_info: Dict[str, Any],
        **kwargs
) -> None:
    """
    Mutates a mapping step (mapping info) by assigning 'mapping_df'.
    """
    found_key = next((k for k in MAPPING_HANDLERS if k in mapping_info), None)
    mapping_info["mapping_df"] = (
        MAPPING_HANDLERS[found_key](
            step=mapping_info,
            **kwargs
        )
        if found_key
        else None
    )


@utility.log_exec_metrics
def generate_step_specific_mapping_dataframes(
    mapping_steps: List[Dict[str, Any]],
    **kwargs
) -> None:
    """
    Mutates each step in mapping_steps by assigning 'mapping_df'.
    """
    for step in mapping_steps:
        generate_dataframe_for_mapping_info(step, **kwargs)


# ------------------------------------------------------------------------------------------------------------
# SQL query to pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def sql_to_dataframe(
        sql_function: Optional[Callable[..., DataFrame]] = None,
        **kwargs: Any
) -> DataFrame:
    """
    Retrieves a DataFrame by executing the provided SQL function

    Args:
        sql_function (Optional[Callable]): A function to execute the SQL query and return a DataFrame.
                                           If None, the default function is used.
        **kwargs (Any): Additional keyword arguments passed to the MDX function.

    Returns:
        DataFrame: The DataFrame resulting from the SQL query.
    """
    if sql_function is None:
        sql_function = __sql_to_dataframe_default

    return sql_function(**kwargs)


def __sql_to_dataframe_default(
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        table_columns: Optional[list] = None,
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        **kwargs
) -> DataFrame:
    if not engine:
        engine = utility.create_sql_engine(**kwargs)

    def fetch(func, **fetch_kwargs):
        return (
            concat(list(func(**fetch_kwargs)), ignore_index=True)
            if chunksize else func(**fetch_kwargs)
        )

    if table_name:
        return fetch(
            read_sql_table, con=engine, table_name=table_name, columns=table_columns, schema=schema, chunksize=chunksize
        )

    if sql_query:
        return fetch(read_sql_query, sql=sql_query, con=engine, chunksize=chunksize)

    msg = "Either 'table_name' or 'sql_query' must be provided."
    basic_logger.error(msg)
    raise ValueError(msg)


# ------------------------------------------------------------------------------------------------------------
# CSV query to pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def csv_to_dataframe(
        csv_function: Optional[Callable[..., DataFrame]] = None,
        **kwargs: Any
) -> DataFrame:
    """
    Retrieves a DataFrame by executing the provided SQL function

    Args:
        csv_function (Optional[Callable]): A function to execute the CSV parsing function and return a DataFrame.
                                           If None, the default function is used.
        **kwargs (Any): Additional keyword arguments.

    Returns:
        DataFrame: The DataFrame resulting from the CSV parsing function.
    """
    if csv_function is None:
        csv_function = __csv_to_dataframe_default

    return csv_function(**kwargs)


def __csv_to_dataframe_default(
        csv_file_path: str,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        dtype: Optional[dict] = None,
        usecols: Optional[UsecolsArgType] = None,
        nrows: Optional[int | None] = None,
        chunksize: Optional[int | None] = None,
        parse_dates: Optional[bool | Sequence[Hashable] | None] = None,
        na_values: Optional[Hashable
                   | Iterable[Hashable]
                   | Mapping[Hashable, Iterable[Hashable]]
                   | None] = None,
        keep_default_na: Optional[bool] = True,
        low_memory: bool = True,
        memory_map: bool = True,
        **kwargs: Any
) -> DataFrame:
    """
    Retrieves a DataFrame from a CSV file.

    Args:
        csv_file_path: (str): The name of the CSV file that is extracted to a DataFrame.
        sep: (Optional[str]): Field delimiter for the output file. If None, it uses the local standard separator.
        decimal: (Optional[str]): Character recognized as decimal separator. If None, it uses the local standard separator.
        dtype: (Optional[dict]): Data types to apply to the whole dataset or individual columns.
        usecols: (Optional[UsecolsArgType]):
                Columns to select.
                If List-like: Elements must be positional or strings matching corresponding column names.
                If Callable: Evaluates the function against the column names.
        nrows: (Optional[int | None]): Number of rows to read.
        chunksize : (Optional[int | None]): Number of rows to read from file per chunk.
        parse_dates: (Optional[bool | Sequence[Hashable] | None]):
        na_values: (Optional[Hashable
                   | Iterable[Hashable]
                   | Mapping[Hashable, Iterable[Hashable]]
                   | None]): Detect missing values
        keep_default_na: (Optional[bool]): Whether or ot to keep NaN values when parsing.
        low_memory: (bool):
                Default True.
                Internally processes the file in chunks. Use with dtype to eliminate possible mixed type inference.
                Reads entire file into one DataFrame.
        memory_map: (bool): If csv_file_path is given it maps the file object directly to memory and eliminates I/O overhead.
        **kwargs (Any): Additional keyword arguments.

    Returns:
        DataFrame: The DataFrame resulting from the CSV file.
    """
    if decimal is None:
        decimal = utility.get_local_decimal_separator()
    if sep is None:
        sep = utility.get_local_regex_separator()

    return DataFrame(pd.read_csv(
        filepath_or_buffer=csv_file_path,
        sep=sep,
        decimal=decimal,
        dtype=dtype,
        usecols=usecols,
        nrows=nrows,
        chunksize=chunksize,
        parse_dates=parse_dates,
        na_values=na_values,
        keep_default_na=keep_default_na,
        low_memory=low_memory,
        memory_map=memory_map,
        **kwargs
    ))
