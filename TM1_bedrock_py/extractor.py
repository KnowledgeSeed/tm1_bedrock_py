from typing import Callable, List, Dict, Optional, Any

from TM1py import TM1Service
from pandas import DataFrame, read_sql_table, read_sql_query

from TM1_bedrock_py import utility, transformer


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
            use_iterative_json=True,
            use_blob=True,
            decimal=utility.get_local_decimal_separator()
        )
    else:
        return tm1_service.cells.execute_mdx_dataframe(
            mdx=data_mdx,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True,
            use_blob=True,
            decimal=utility.get_local_decimal_separator()
        )


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
    dataframe = tm1_mdx_to_dataframe(
        mdx_function=mdx_function,
        tm1_service=tm1_service,
        data_mdx=mdx,
        skip_zeros=True,
        skip_consolidated_cells=True,
        **kwargs
    )
    metadata_object = utility.TM1CubeObjectMetadata.collect(
        metadata_function=step.get("mapping_metadata_function"),
        tm1_service=tm1_service,
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
    transformer.normalize_sql_dataframe(
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


#@utility.log_exec_metrics
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
    if table_name:
        return read_sql_table(
            con=engine, table_name=table_name, columns=table_columns, schema=schema, chunksize=chunksize
        )
    if sql_query:
        return read_sql_query(sql=sql_query, con=engine, chunksize=chunksize)
