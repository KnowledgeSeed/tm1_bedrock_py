from typing import Callable, List, Optional, Any, Literal

from TM1py import TM1Service
from pandas import DataFrame
from sqlalchemy import text
from TM1_bedrock_py import utility


# ------------------------------------------------------------------------------------------------------------
# Main: pandas dataframe to cube functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
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
        clear_function = __clear_cube_default
    return clear_function(**kwargs)


def __clear_cube_default(
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
    clearing_kwargs = utility.__transform_set_mdx_list_to_tm1py_clear_kwargs(clear_set_mdx_list)
    tm1_service.cells.clear(cube_name, **clearing_kwargs)


@utility.log_exec_metrics
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
        write_function = __dataframe_to_cube_default
    return write_function(**kwargs)


def __dataframe_to_cube_default(
        tm1_service: TM1Service,
        dataframe: DataFrame,
        cube_name: str,
        cube_dims: List[str],
        use_blob: bool,
        slice_size_of_dataframe: int,
        async_write: bool = False,
        use_ti: bool = False,
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
    if async_write:
        tm1_service.cells.write_dataframe_async(
            cube_name=cube_name,
            data=dataframe,
            dimensions=cube_dims,
            deactivate_transaction_log=True,
            reactivate_transaction_log=True,
            skip_non_updateable=True,
            increment=increment,
            sum_numeric_duplicates=sum_numeric_duplicates,
            slice_size_of_dataframe=slice_size_of_dataframe,
            **kwargs
        )
    else:
        tm1_service.cells.write_dataframe(
            cube_name=cube_name,
            data=dataframe,
            dimensions=cube_dims,
            deactivate_transaction_log=True,
            reactivate_transaction_log=True,
            skip_non_updateable=True,
            use_ti=use_ti,
            use_blob=use_blob,
            remove_blob=True,
            increment=increment,
            sum_numeric_duplicates=sum_numeric_duplicates,
            **kwargs
        )


# ------------------------------------------------------------------------------------------------------------
# pandas dataframe into SQL functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def dataframe_to_sql(
        sql_write_function: Optional[Callable[..., DataFrame]] = None,
        **kwargs: Any
) -> None:
    """
    Retrieves a DataFrame by executing the provided SQL function

    Args:
        sql_write_function (Optional[Callable]):
            A function to write a dataframe into SQL
            If None, the default function is used.
        **kwargs (Any): Additional keyword arguments passed to the MDX function.

    Returns:
        DataFrame: The DataFrame resulting from the SQL query.
    """
    if sql_write_function is None:
        sql_write_function = __dataframe_to_sql_default

    return sql_write_function(**kwargs)


def __dataframe_to_sql_default(
        dataframe: DataFrame,
        table_name: str,
        engine: Optional[Any] = None,
        if_exists: Literal["fail", "replace", "append"] = "append",
        index: Optional[bool] = False,
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[dict] = None,
        method: Optional[Any] = None,
        **kwargs
) -> None:
    if not engine:
        engine = utility.create_sql_engine(**kwargs)
    dataframe.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        schema=schema,
        chunksize=chunksize,
        dtype=dtype,
        method=method,
        index=index
    )


# ------------------------------------------------------------------------------------------------------------
# pandas dataframe into CSV functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def dataframe_to_csv(
        dataframe: DataFrame,
        csv_file_name: str,
        mode: str = "a",
        chunksize: int | None = None,
        **kwargs
) -> None:
    """
      Retrieves a DataFrame by executing the provided SQL function

      Args:
          dataframe (DataFrame): A DataFrame that is to be written into a CSV file.
          csv_file_name (str): The name of the CSV file that is written into.
          mode : {{'w', 'x', 'a'}}, default 'w'
            Forwarded to either `open(mode=)` or `fsspec.open(mode=)` to control
            the file opening. Typical values include:
            - 'w', truncate the file first.
            - 'x', exclusive creation, failing if the file already exists.
            - 'a', append to the end of file if it exists.
          chunksize : int or None
            Rows to write at a time.
          **kwargs (Any): Additional keyword arguments.

      Returns:
          None
      """
    dataframe.to_csv(path_or_buf=csv_file_name, mode=mode, chunksize=chunksize, index=False)


@utility.log_exec_metrics
def clear_table(
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
        clear_function = __clear_table_default
    return clear_function(**kwargs)


def __clear_table_default(
        engine: Any,
        table_name: Optional[str],
        delete_statement: Optional[str]
) -> None:
    with engine.connect() as connection:
        if table_name:
            connection.execute(text("TRUNCATE TABLE [" + table_name + "]"))
        elif delete_statement:
            connection.execute(text(delete_statement))


