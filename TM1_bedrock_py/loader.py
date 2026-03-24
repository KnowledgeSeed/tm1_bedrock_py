from typing import Callable, List, Optional, Any, Literal, Union
from TM1py import TM1Service
from pandas import DataFrame
from sqlalchemy import text
from TM1_bedrock_py import utility, basic_logger


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
        clear_set_mdx_list: List[str],
        **_kwargs
) -> None:
    """
    Clears a cube with filters by generating clear parameters from a list of set MDXs.

    Args:
        tm1_service (TM1Service): An active TM1Service object for the TM1 server connection.
        cube_name (str): The name of the cube to clear.
        clear_set_mdx_list (List[str]): A list of valid MDX set expressions defining the clear space.
        **_kwargs (Any): Additional keyword arguments.
    """
    clearing_kwargs = utility.get_kwargs_dict_from_set_mdx_list(clear_set_mdx_list) \
        if clear_set_mdx_list is not None else {}

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

    dataframe = kwargs.get("dataframe")
    write_function(**kwargs)
    basic_logger.info("Writing of "+str(len(dataframe))+" rows into tm1 is complete.")


def __dataframe_to_cube_default(
        tm1_service: TM1Service,
        dataframe: DataFrame,
        cube_name: str,
        cube_dims: List[str],
        use_blob: bool,
        slice_size_of_dataframe: int = 250_000,
        async_write: bool = False,
        use_ti: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        remove_blob: Optional[bool] = True,
        **_kwargs
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
        **_kwargs (Any): Additional keyword arguments.

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
            allow_spread=False
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
            remove_blob=remove_blob,
            increment=increment,
            sum_numeric_duplicates=sum_numeric_duplicates,
            allow_spread=False
        )


# ------------------------------------------------------------------------------------------------------------
# pandas dataframe into SQL functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def dataframe_to_sql(
        sql_function: Optional[Union[Callable[..., DataFrame], Literal["sqlalchemy", "pyodbc", "psycopg2", "snowflake"]]] = None,
        **kwargs: Any
) -> None:
    """
    Retrieves a DataFrame by executing the provided SQL function

    Args:
        sql_function (Optional[Callable]):
            A function to write a dataframe into SQL
            If None, the default function is used.
        **kwargs (Any): Additional keyword arguments passed to the MDX function.

    Returns:
        DataFrame: The DataFrame resulting from the SQL query.
    """
    writer_function = sql_function if isinstance(sql_function, Callable) else {
        None: __dataframe_to_sql_default,
        "sqlalchemy": __dataframe_to_sql_default,
        "pyodbc": __dataframe_to_sql_pyodbc,
        "psycopg2": __dataframe_to_sql_psycopg2,
        "snowflake": __dataframe_to_sql_snowflake
    }.get(sql_function)

    dataframe = kwargs.get("dataframe")
    writer_function(**kwargs)
    basic_logger.info("Writing of " + str(len(dataframe)) + " rows into sql is complete.")


def __dataframe_to_sql_default(
        dataframe: DataFrame,
        table_name: str,
        engine: Optional[Any] = None,
        if_exists: Literal["fail", "replace", "append"] = "append",
        index: Optional[bool] = False,
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[dict] = None,
        method: Optional[Union[str, Callable]] = None,
        table_column_order: list[str] = None,
        **kwargs
) -> None:
    if not engine:
        engine = utility.create_sql_engine(**kwargs)

    if table_column_order is None:
        table_column_order = utility.inspect_table(engine, table_name=table_name, schema=schema)
        column_order = [col.get('name') for col in table_column_order]
    else:
        column_order = table_column_order

    df_cols = list(dataframe.columns)
    column_order = [c for c in column_order if c in df_cols]

    dataframe = dataframe[column_order]
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


def __build_sql_create_table_statement(
        dataframe_ordered: DataFrame,
        full_table_name: str,
        quote_identifier: Callable[[str], str],
        pandas_to_sql_type_map: dict[str, str],
        default_sql_type: str,
        dtype: Optional[dict[str, str]]
) -> str:
    column_definitions_list = [
        f"{quote_identifier(column_name)} "
        f"{dtype.get(column_name) if dtype and column_name in dtype else pandas_to_sql_type_map.get(str(dataframe_ordered.dtypes[column_name]), default_sql_type)}"
        for column_name in dataframe_ordered.columns
    ]
    column_definitions_string = ",\n    ".join(column_definitions_list)
    return f"CREATE TABLE {full_table_name} (\n    {column_definitions_string}\n)"


def __initialize_sql_table_if_needed(
        database_cursor: Any,
        database_connection: Any,
        table_exists: bool,
        if_exists: str,
        full_table_name: str,
        create_table_statement: str
) -> None:
    execution_routing_map: dict[tuple[bool, str], Optional[str]] = {
        (True, "replace_data"): f"TRUNCATE TABLE {full_table_name}",
        (True, "replace_table"): f"DROP TABLE {full_table_name}; {create_table_statement}",
        (True, "append"): None,
        (True, "fail"): None,
        (False, "replace_data"): create_table_statement,
        (False, "replace_table"): create_table_statement,
        (False, "append"): create_table_statement,
        (False, "fail"): create_table_statement
    }

    if table_exists and if_exists == "fail":
        raise ValueError(f"Table '{full_table_name}' already exists.")

    initialization_sql_statement = execution_routing_map.get((table_exists, if_exists))
    if initialization_sql_statement is not None:
        database_cursor.execute(initialization_sql_statement)
        database_connection.commit()


def __write_dataframe_sql_api(
        dataframe: DataFrame,
        table_name: str,
        database_engine_or_connection: Any,
        if_exists: Literal["fail", "replace_data", "replace_table", "append"] = "append",
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[dict[str, str]] = None,
        table_column_order: Optional[list[str]] = None,
        default_schema: str = "",
        quote_identifier: Optional[Callable[[str], str]] = None,
        pandas_to_sql_type_map: Optional[dict[str, str]] = None,
        default_sql_type: str = "TEXT",
        table_exists_query: str = "",
        table_exists_params: Optional[Callable[[str, str], tuple[Any, ...]]] = None,
        insert_placeholder: str = "%s",
        use_fast_executemany: bool = False,
        use_multi_value_insert: bool = False,
        max_statement_parameters: Optional[int] = None
) -> None:
    database_connection, owns_connection = __get_sql_api_connection(database_engine_or_connection)
    database_cursor = database_connection.cursor()

    try:
        column_order = table_column_order if table_column_order is not None else list(dataframe.columns)
        dataframe_ordered = dataframe[column_order]

        target_schema = schema if schema is not None else default_schema
        quote_identifier = quote_identifier or (lambda identifier: identifier)
        pandas_to_sql_type_map = pandas_to_sql_type_map or {}
        table_exists_params = table_exists_params or (lambda current_schema, current_table: (current_schema, current_table))

        full_table_name = f"{quote_identifier(target_schema)}.{quote_identifier(table_name)}"

        database_cursor.execute(table_exists_query, table_exists_params(target_schema, table_name))
        table_exists = bool(database_cursor.fetchone())

        create_table_statement = __build_sql_create_table_statement(
            dataframe_ordered=dataframe_ordered,
            full_table_name=full_table_name,
            quote_identifier=quote_identifier,
            pandas_to_sql_type_map=pandas_to_sql_type_map,
            default_sql_type=default_sql_type,
            dtype=dtype
        )

        __initialize_sql_table_if_needed(
            database_cursor=database_cursor,
            database_connection=database_connection,
            table_exists=table_exists,
            if_exists=if_exists,
            full_table_name=full_table_name,
            create_table_statement=create_table_statement
        )

        target_columns_formatted_string = ", ".join(
            [quote_identifier(column_name) for column_name in dataframe_ordered.columns]
        )
        value_placeholders_string = ", ".join([insert_placeholder] * len(dataframe_ordered.columns))
        sql_insert_statement = (
            f"INSERT INTO {full_table_name} ({target_columns_formatted_string}) "
            f"VALUES ({value_placeholders_string})"
        )

        dataframe_sanitized = dataframe_ordered.astype(object).where(dataframe_ordered.notnull(), None)
        columns_count = len(dataframe_ordered.columns)

        cursor_supports_fast_executemany = False
        if use_fast_executemany and hasattr(database_cursor, "fast_executemany"):
            try:
                database_cursor.fast_executemany = True
                cursor_supports_fast_executemany = True
            except (AttributeError, Exception):
                cursor_supports_fast_executemany = False

        if use_multi_value_insert and not cursor_supports_fast_executemany and max_statement_parameters:
            chunk_size_default = max(1, max_statement_parameters // max(columns_count, 1))
        elif cursor_supports_fast_executemany:
            chunk_size_default = 10000
        else:
            chunk_size_default = 10000

        processing_chunk_size = chunksize if chunksize is not None else chunk_size_default

        sql_base = f"INSERT INTO {full_table_name} ({target_columns_formatted_string}) VALUES "
        for chunk_start_index in range(0, len(dataframe_sanitized), processing_chunk_size):
            chunk: DataFrame = dataframe_sanitized.iloc[chunk_start_index: chunk_start_index + processing_chunk_size]

            if use_multi_value_insert and not cursor_supports_fast_executemany:
                placeholders = ", ".join(
                    [f"({', '.join([insert_placeholder] * columns_count)})"] * len(chunk)
                )
                values_flattened: list[Any] = chunk.to_numpy().flatten().tolist()
                database_cursor.execute(f"{sql_base}{placeholders}", values_flattened)
            else:
                database_cursor.executemany(sql_insert_statement, chunk.to_numpy().tolist())

        database_connection.commit()
    finally:
        database_cursor.close()
        if owns_connection and hasattr(database_connection, "close"):
            database_connection.close()


def __get_sql_api_connection(database_engine_or_connection: Any) -> tuple[Any, bool]:
    if hasattr(database_engine_or_connection, "cursor"):
        return database_engine_or_connection, False

    if hasattr(database_engine_or_connection, "raw_connection"):
        return database_engine_or_connection.raw_connection(), True

    if hasattr(database_engine_or_connection, "connection") and hasattr(database_engine_or_connection.connection, "cursor"):
        return database_engine_or_connection.connection, False

    if hasattr(database_engine_or_connection, "connect"):
        sql_connection = database_engine_or_connection.connect()
        if hasattr(sql_connection, "connection") and hasattr(sql_connection.connection, "cursor"):
            return sql_connection.connection, True
        return sql_connection, True

    return database_engine_or_connection, False


def __dataframe_to_sql_pyodbc(
        dataframe: DataFrame,
        table_name: str,
        database_engine_or_connection: Any,
        if_exists: Literal["fail", "replace_data","replace_table", "append"] = "append",
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[dict[str, str]] = None,
        table_column_order: Optional[list[str]] = None,
        **_kwargs
) -> None:
    __write_dataframe_sql_api(
        dataframe=dataframe,
        table_name=table_name,
        database_engine_or_connection=database_engine_or_connection,
        if_exists=if_exists,
        schema=schema,
        chunksize=chunksize,
        dtype=dtype,
        table_column_order=table_column_order,
        default_schema="dbo",
        quote_identifier=lambda identifier: f"[{identifier}]",
        pandas_to_sql_type_map={
            "int64": "BIGINT",
            "int32": "INT",
            "float64": "FLOAT",
            "float32": "REAL",
            "bool": "BIT",
            "datetime64[ns]": "DATETIME2",
            "object": "NVARCHAR(MAX)",
            "string": "NVARCHAR(MAX)"
        },
        default_sql_type="NVARCHAR(MAX)",
        table_exists_query="SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        insert_placeholder="?",
        use_fast_executemany=True,
        use_multi_value_insert=True,
        max_statement_parameters=2100
    )


def __dataframe_to_sql_psycopg2(
        dataframe: DataFrame,
        table_name: str,
        database_engine_or_connection: Any,
        if_exists: Literal["fail", "replace_data", "replace_table", "append"] = "append",
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[dict[str, str]] = None,
        table_column_order: Optional[list[str]] = None,
        **_kwargs
) -> None:
    __write_dataframe_sql_api(
        dataframe=dataframe,
        table_name=table_name,
        database_engine_or_connection=database_engine_or_connection,
        if_exists=if_exists,
        schema=schema,
        chunksize=chunksize,
        dtype=dtype,
        table_column_order=table_column_order,
        default_schema="public",
        quote_identifier=lambda identifier: '"' + identifier.replace('"', '""') + '"',
        pandas_to_sql_type_map={
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "object": "TEXT",
            "string": "TEXT"
        },
        default_sql_type="TEXT",
        table_exists_query="SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
        insert_placeholder="%s"
    )


def __dataframe_to_sql_snowflake(
        dataframe: DataFrame,
        table_name: str,
        database_engine_or_connection: Any,
        if_exists: Literal["fail", "replace_data", "replace_table", "append"] = "append",
        schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        dtype: Optional[dict[str, str]] = None,
        table_column_order: Optional[list[str]] = None,
        **_kwargs
) -> None:
    __write_dataframe_sql_api(
        dataframe=dataframe,
        table_name=table_name,
        database_engine_or_connection=database_engine_or_connection,
        if_exists=if_exists,
        schema=schema,
        chunksize=chunksize,
        dtype=dtype,
        table_column_order=table_column_order,
        default_schema="PUBLIC",
        quote_identifier=lambda identifier: '"' + identifier.replace('"', '""') + '"',
        pandas_to_sql_type_map={
            "int64": "NUMBER",
            "int32": "NUMBER",
            "float64": "FLOAT",
            "float32": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP_NTZ",
            "object": "VARCHAR",
            "string": "VARCHAR"
        },
        default_sql_type="VARCHAR",
        table_exists_query="SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
        table_exists_params=lambda current_schema, current_table: (current_schema.upper(), current_table.upper()),
        insert_placeholder="%s"
    )


@utility.log_exec_metrics
def clear_table(
        clear_function: Optional[Union[Callable[..., Any], Literal["sqlalchemy", "pyodbc", "psycopg2", "snowflake"]]] = None,
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
    clear_function = clear_function if isinstance(clear_function, Callable) else {
        None: __clear_table_default,
        "sqlalchemy": __clear_table_default,
        "pyodbc": __clear_table_pyodbc,
        "psycopg2": __clear_table_psycopg2,
        "snowflake": __clear_table_snowflake
    }.get(clear_function)

    clear_function(**kwargs)


def __clear_table_default(
        database_engine_or_connection: Any,
        table_name: Optional[str],
        delete_statement: Optional[str],
        **_kwargs
) -> None:
    connection, owns_connection = __get_sql_api_connection(database_engine_or_connection)

    try:
        if hasattr(connection, "begin") and hasattr(connection, "execute"):
            transaction = connection.begin()
            if delete_statement:
                connection.execute(text(delete_statement))
            elif table_name:
                connection.execute(text("TRUNCATE TABLE [" + table_name + "]"))
            transaction.commit()
        else:
            with connection.cursor() as cursor:
                if delete_statement:
                    cursor.execute(delete_statement)
                elif table_name:
                    cursor.execute("TRUNCATE TABLE [" + table_name + "]")
                connection.commit()
    finally:
        if owns_connection and hasattr(connection, "close"):
            connection.close()


def __clear_table_pyodbc(
        database_engine_or_connection: Any,
        table_name: Optional[str],
        delete_statement: Optional[str],
        **_kwargs
) -> None:
    connection, owns_connection = __get_sql_api_connection(database_engine_or_connection)
    statement: str = delete_statement or f"TRUNCATE TABLE [{table_name}]"

    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            connection.commit()
    finally:
        if owns_connection and hasattr(connection, "close"):
            connection.close()


def __clear_table_psycopg2(
        database_engine_or_connection: Any,
        table_name: Optional[str],
        schema_name: Optional[str],
        delete_statement: Optional[str],
        **_kwargs
) -> None:
    connection, owns_connection = __get_sql_api_connection(database_engine_or_connection)
    statement: str = delete_statement or f"TRUNCATE TABLE {schema_name}.{table_name}"

    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            connection.commit()
    finally:
        if owns_connection and hasattr(connection, "close"):
            connection.close()


def __clear_table_snowflake(
        database_engine_or_connection: Any,
        table_name: Optional[str],
        schema_name: Optional[str],
        delete_statement: Optional[str],
        **_kwargs
) -> None:
    connection, owns_connection = __get_sql_api_connection(database_engine_or_connection)
    statement: str = delete_statement or f'TRUNCATE TABLE "{schema_name or "PUBLIC"}"."{table_name}"'

    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            connection.commit()
    finally:
        if owns_connection and hasattr(connection, "close"):
            connection.close()

# ------------------------------------------------------------------------------------------------------------
# pandas dataframe into CSV functions
# ------------------------------------------------------------------------------------------------------------


@utility.log_exec_metrics
def dataframe_to_csv(
        dataframe: DataFrame,
        csv_file_name: str,
        csv_output_dir: Optional[str] = None,
        chunksize: Optional[int] = None,
        float_format: Optional[Union[str, Callable]] = None,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        na_rep: Optional[str] = "NULL",
        compression: Optional[Union[str, dict]] = None,
        index: Optional[bool] = False,
        mode: str = "w",
        **_kwargs
) -> None:
    """
      Retrieves a DataFrame by executing the provided SQL function

      Args:
          csv_output_dir:
          dataframe (DataFrame): A DataFrame that is to be written into a CSV file.
          csv_file_name (str): The name of the CSV file that is written into.
          mode : {{'w', 'x', 'a'}}, default 'w'
            Forwarded to either `open(mode=)` or `fsspec.open(mode=)` to control
            the file opening. Typical values include:
            - 'w', truncate the file first.
            - 'x', exclusive creation, failing if the file already exists.
            - 'a', append to the end of file if it exists.
          chunksize : (Optional[int | None]): Rows to write at a time.
          float_format: (Optional[str]): Floating point format.
            Callable takes precedence over other numeric formatting like decimal.
          sep: (Optional[str]): Field delimiter for the output file. If None, it uses the local standard separator.
          decimal: (Optional[str]): Character recognized as decimal separator.
            If None, it uses the local standard separator.
          na_rep: (Optional[str]): Missing data representation. Defaults to NULL.
          compression: (Optional[str | dict]): For on-the-fly compression of the output data.
          index: (Optional[bool]): Default False. If True, writes row indices.
          **_kwargs (Any): Additional keyword arguments.

      Returns:
          None
      """
    if decimal is None:
        decimal = utility.get_local_decimal_separator()
    if sep is None:
        sep = utility.get_local_regex_separator()

    if csv_output_dir is None:
        csv_output_dir = "./dataframe_to_csv"

    filepath = utility.generate_valid_file_path(output_dir=csv_output_dir, filename=csv_file_name)

    dataframe.to_csv(
        path_or_buf=filepath,
        mode=mode,
        chunksize=chunksize,
        float_format=float_format,
        sep=sep,
        decimal=decimal,
        na_rep=na_rep,
        compression=compression,
        index=index
    )
