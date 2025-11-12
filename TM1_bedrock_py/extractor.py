from typing import Callable, List, Dict, Optional, Any, Literal

from TM1py import TM1Service, NativeView, Subset
from pandas import DataFrame, read_sql_table, read_sql_query, concat, read_csv
from sqlalchemy import text
from typing import Sequence, Hashable, Mapping, Iterable
import random, string
from TM1_bedrock_py import utility, transformer, basic_logger


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def tm1_mdx_to_dataframe(
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
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
    elif mdx_function == "native_view_extractor":
        mdx_function = __tm1_mdx_to_native_view_to_dataframe
        basic_logger.info("Native view extraction mode enabled.")

    dataframe = mdx_function(**kwargs)
    basic_logger.info("Extracted " + str(len(dataframe)) + " rows from tm1 datasource")
    dataframe.normalized = False
    return dataframe


def __tm1_mdx_to_dataframe_default(
        tm1_service: TM1Service,
        data_mdx: Optional[str] = None,
        data_mdx_list:  Optional[list[str]] = None,
        skip_zeros: bool = False,
        skip_consolidated_cells: bool = False,
        skip_rule_derived_cells: bool = False,
        decimal: str = None,
        use_blob: Optional[bool] = True,
        **_kwargs
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
    if decimal is None:
        decimal = utility.get_local_decimal_separator()

    if data_mdx_list:
        if skip_zeros:
            data_mdx_list = [utility.add_non_empty_to_mdx(current)
                             for current in data_mdx_list]

        return tm1_service.cells.execute_mdx_dataframe_async(
            mdx_list=data_mdx_list,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True,
            decimal=decimal
        )
    elif data_mdx:
        if skip_zeros:
            data_mdx = utility.add_non_empty_to_mdx(data_mdx)

        return tm1_service.cells.execute_mdx_dataframe(
            mdx=data_mdx,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True,
            use_blob=use_blob,
            decimal=decimal
        )

    msg = "Either data_mdx or data_mdx_list has to be specified."
    basic_logger.error(msg)
    raise ValueError(msg)


def __tm1_mdx_to_native_view_to_dataframe(
        tm1_service: TM1Service,
        data_mdx: Optional[str] = None,
        skip_zeros: bool = False,
        skip_consolidated_cells: bool = False,
        skip_rule_derived_cells: bool = False,
        decimal: str = None,
        use_blob: Optional[bool] = True,
        view_and_subset_cleanup: Optional[bool] = True,
        **_kwargs
) -> DataFrame:
    if decimal is None:
        decimal = utility.get_local_decimal_separator()

    cube_name = utility.get_cube_name_from_mdx(mdx_query=data_mdx)
    view_name = "tm1_bedrock_py_"+"".join(random.choices(string.ascii_lowercase + string.digits, k=16))
    set_mdx_list = utility.extract_mdx_components(mdx=data_mdx)
    set_mdx_dimensions = utility.get_dimensions_from_set_mdx_list(mdx_sets=set_mdx_list)
    set_mdx_elements = utility.generate_element_lists_from_set_mdx_list(tm1_service=tm1_service,
                                                                        set_mdx_list=set_mdx_list)

    native_view = NativeView(cube_name=cube_name, view_name=view_name,
                             suppress_empty_rows=skip_zeros, suppress_empty_columns=skip_zeros)
    native_view.suppress_empty_cells = skip_zeros
    for i, (dimension_name, set_mdx, elements) in enumerate(zip(set_mdx_dimensions, set_mdx_list, set_mdx_elements)):
        subset = Subset(subset_name=view_name,
                        dimension_name=dimension_name)
        subset.add_elements(elements=elements)
        tm1_service.subsets.create(subset=subset)
        if i == 0:
            native_view.add_column(dimension_name=dimension_name, subset=subset)
        else:
            native_view.add_row(dimension_name=dimension_name, subset=subset)
    tm1_service.views.create(view=native_view)
    basic_logger.info("View and dimension subsets named " + view_name + " were created.")

    column_dimension_list = [f"[{set_mdx_dimensions[0]}].[{set_mdx_dimensions[0]}]"]
    row_dimension_list = [f"[{d}].[{d}]" for d in set_mdx_dimensions[1:]]

    dataframe = tm1_service.cells.execute_view_dataframe(
        cube_name=cube_name,
        view_name=view_name,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        use_blob=use_blob,
        arranged_axes=([], row_dimension_list, column_dimension_list),
        decimal=decimal
    )
    try:
        return dataframe
    finally:
        if view_and_subset_cleanup:
            tm1_service.views.delete(cube_name=cube_name, view_name=view_name)
            for dimension_name in set_mdx_dimensions:
                tm1_service.subsets.delete(subset_name=view_name, dimension_name=dimension_name)
            basic_logger.info("View and dimension subsets named " + view_name + " were deleted.")


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
    mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
    tm1_service: Optional[Any] = None,
    **kwargs
) -> DataFrame:
    """Execute MDX and augment the resulting DataFrame with metadata."""
    native_view_extraction_enabled = mdx_function == "native_view_extractor"

    mdx = step["mapping_mdx"]
    step_specific_tm1_service = step.get("tm1_service")

    kwargs_copy = kwargs.copy()
    kwargs_copy.pop("skip_zeros", None)
    kwargs_copy.pop("skip_consolidated_cells", None)

    if not step_specific_tm1_service:
        step_specific_tm1_service = tm1_service
    dataframe = tm1_mdx_to_dataframe(
        mdx_function=mdx_function,
        tm1_service=step_specific_tm1_service,
        data_mdx=mdx,
        skip_zeros=True,
        skip_consolidated_cells=True,
        **kwargs_copy
    )
    metadata_object = utility.TM1CubeObjectMetadata.collect(
        metadata_function=step.get("mapping_metadata_function"),
        tm1_service=step_specific_tm1_service,
        mdx=mdx,
        collect_base_cube_metadata=False,
        collect_source_cube_metadata=native_view_extraction_enabled,
        **kwargs_copy
    )
    filter_dict = metadata_object.get_filter_dict()
    if native_view_extraction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=metadata_object.get_source_cube_dims()
        )
    transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value=filter_dict)

    return dataframe


def _handle_mapping_sql_query(
        step: Dict[str, Any],
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable] = None,
        **kwargs
) -> DataFrame:
    table_name = step.get("mapping_sql_table_name")
    schema = step.get("sql_schema")
    sql_query = step.get("mapping_sql_query")
    dataframe = sql_to_dataframe(
        sql_function=sql_function, engine=sql_engine,
        sql_query=sql_query, schema=schema, table_name=table_name,
        **kwargs
    )
    column_mapping = step.get("column_mapping")
    columns_to_drop = step.get("columns_to_drop")

    transformer.normalize_table_source_dataframe(
        dataframe=dataframe,
        column_mapping=column_mapping,
        columns_to_drop=columns_to_drop
    )
    return dataframe


def _handle_mapping_csv(
        step: Dict[str, Any],
        csv_function: Optional[Callable] = None,
        **_kwargs
) -> None:
    """
        csv_file_path: str,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        dtype: Optional[dict] = None,
        chunksize: Optional[int | None] = None,
    """
    csv_file_path = step["mapping_csv_file_path"]
    sep = step.get("csv_separator")
    decimal = step.get("csv_decimal")
    dtype = step.get("csv_dtype")
    chunksize = step.get("csv_chunksize")
    dataframe = csv_to_dataframe(
        csv_function=csv_function,
        csv_file_path=csv_file_path,
        sep=sep, decimal=decimal,
        dtype=dtype,
        chunksize=chunksize
    )

    column_mapping = step.get("column_mapping")
    columns_to_drop = step.get("columns_to_drop")

    transformer.normalize_table_source_dataframe(
        dataframe=dataframe,
        column_mapping=column_mapping,
        columns_to_drop=columns_to_drop
    )


MAPPING_HANDLERS = {
    "mapping_df": _handle_mapping_df,
    "mapping_mdx": _handle_mapping_mdx,
    "mapping_sql_query": _handle_mapping_sql_query,
    "mapping_sql_table_name": _handle_mapping_sql_query,
    "mapping_csv_file_path": _handle_mapping_csv
}


@utility.log_exec_metrics
def generate_dataframe_for_mapping_info(
        mapping_info: Dict[str, Any],
        step_specific_string: Optional[str] = "shared",
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
    utility.dataframe_verbose_logger(
        dataframe=mapping_info["mapping_df"],
        step_number=f"mapping_step_{step_specific_string}",
        **kwargs
    )


@utility.log_exec_metrics
def generate_step_specific_mapping_dataframes(
    mapping_steps: List[Dict[str, Any]],
    **kwargs
) -> None:
    """
    Mutates each step in mapping_steps by assigning 'mapping_df'.
    """
    if not mapping_steps:
        return
    for i, step in enumerate(mapping_steps):
        generate_dataframe_for_mapping_info(mapping_info=step, step_specific_string=str(i+1), **kwargs)


# ------------------------------------------------------------------------------------------------------------
# SQL query to pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------

def _get_sql_table_count(engine: Any, table_name: str) -> int:
    """Gets the total row count of a SQL table."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar_one()
    except Exception as e:
        basic_logger.error(f"Failed to get row count for table '{table_name}': {e}")
        return 0


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

    dataframe = sql_function(**kwargs)
    basic_logger.info("Extracted " + str(len(dataframe)) + " rows from sql datasource.")
    dataframe.normalized = False
    return dataframe


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
        return (concat(list(func(**fetch_kwargs)), ignore_index=True)
                if chunksize else func(**fetch_kwargs))

    if table_name:
        return fetch(read_sql_table,
                     con=engine,
                     table_name=table_name,
                     columns=table_columns,
                     schema=schema,
                     chunksize=chunksize)

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

    dataframe = csv_function(**kwargs)
    basic_logger.info("Extracted " + str(len(dataframe)) + " rows from csv datasource.")
    dataframe.normalized = False
    return dataframe


def __csv_to_dataframe_default(
        csv_file_path: str,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        dtype: Optional[dict] = None,
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
        **_kwargs: Any
) -> DataFrame:
    """
    Retrieves a DataFrame from a CSV file.

    Args:
        csv_file_path: (str): The name of the CSV file that is extracted to a DataFrame.
        sep: (Optional[str]): Field delimiter for the output file. If None, it uses the local standard separator.
        decimal: (Optional[str]): Character recognized as decimal separator.
                If None, it uses the local standard separator.
        dtype: (Optional[dict]): Data types to apply to the whole dataset or individual columns.
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
        memory_map: (bool):
            If csv_file_path is given it maps the file object directly to memory and eliminates I/O overhead.
        **_kwargs (Any): Additional keyword arguments.

    Returns:
        DataFrame: The DataFrame resulting from the CSV file.
    """
    if decimal is None:
        decimal = utility.get_local_decimal_separator()
    if sep is None:
        sep = utility.get_local_regex_separator()

    if chunksize:
        chunks = []
        for chunk in read_csv(
            filepath_or_buffer=csv_file_path,
            sep=sep,
            decimal=decimal,
            dtype=dtype,
            nrows=nrows,
            chunksize=chunksize,
            parse_dates=parse_dates,
            na_values=na_values,
            keep_default_na=keep_default_na,
            low_memory=low_memory,
            memory_map=memory_map
        ):
            chunks.append(chunk)
        return concat(chunks, ignore_index=True)
    else:
        return read_csv(
            filepath_or_buffer=csv_file_path,
            sep=sep,
            decimal=decimal,
            dtype=dtype,
            nrows=nrows,
            parse_dates=parse_dates,
            na_values=na_values,
            keep_default_na=keep_default_na,
            low_memory=low_memory,
            memory_map=memory_map
        )
