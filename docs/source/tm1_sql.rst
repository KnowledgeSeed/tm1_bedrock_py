SQL Integration
===============

The SQL wrappers are first-class Bedrock workflows, not add-ons. They let you
reuse the same mapping, validation, and write concepts you use for TM1-to-TM1
loads while integrating with warehouse or staging-table data.

SQL to TM1
----------

``load_sql_data_to_tm1_cube`` reads either ``sql_query`` or
``sql_table_name`` through a supplied ``sql_engine`` or custom
``sql_function``.

.. code-block:: python

   from sqlalchemy import create_engine

   engine = create_engine("mssql+pyodbc://...")

   bedrock.load_sql_data_to_tm1_cube(
       tm1_service=tm1,
       sql_engine=engine,
       sql_query="""
           SELECT Version, Period, Product, Amount AS Value
           FROM dbo.FactSales
           WHERE LoadBatch = 42
       """,
       target_cube_name="Sales",
       sql_column_mapping={"ProductCode": "Product"},
       sql_columns_to_drop=["LoadBatch"],
       clear_target=True,
       target_clear_set_mdx_list=[
           "{[Version].[Version].[Actual]}",
       ],
       async_write=True,
   )

Source parameters:

``sql_query`` / ``sql_table_name``
   Provide exactly the source needed by the SQL extractor. Table extraction can
   also use ``sql_table_columns`` and ``sql_schema``.

``sql_column_mapping``
   Rename source columns to TM1 dimension names or ``Value``.

``sql_columns_to_drop``
   Remove source-only columns before transformation.

``chunksize``
   Read SQL in chunks where supported by the extractor.

``clear_source``
   Clear the SQL source only after a successful, non-empty TM1 load. Supply
   ``sql_delete_statement`` for portable, explicit behavior.

This wrapper is the main choice when SQL is the source of truth and TM1 is the
serving layer.

TM1 to SQL
----------

``load_tm1_cube_to_sql_table`` extracts TM1 data, transforms it, and writes a
SQL table.

.. code-block:: python

   bedrock.load_tm1_cube_to_sql_table(
       tm1_service=tm1,
       data_mdx="""
       SELECT {[Period].[Period].[2026-01]} ON 0
       FROM [Sales]
       WHERE ([Version].[Version].[Actual])
       """,
       sql_engine=engine,
       target_table_name="FactSalesExport",
       sql_column_mapping={"Value": "Amount"},
       if_table_exists="append",
       dtype={"Amount": "DECIMAL(18, 2)"},
       clear_target=False,
       skip_zeros=True,
   )

Target parameters:

``sql_engine`` / ``sql_connection``
   SQLAlchemy engine/connection or compatible DB-API connection.

``sql_function``
   A callable or built-in route: ``"sqlalchemy"``, ``"pyodbc"``,
   ``"psycopg2"``, or ``"snowflake"``.

``if_table_exists``
   ``"fail"``, ``"replace_data"``, ``"replace_table"``, or ``"append"``.

``dtype``
   Explicit SQL type mapping. SQLAlchemy and direct DB-API writers interpret
   types according to their own route.

``clear_target``
   Clear the SQL target immediately before writing. Prefer an explicit
   ``sql_delete_statement`` when database truncate syntax differs.

``clear_function``
   A callable or one of the same built-in backend routes.

``clear_source``
   Clear the TM1 source slice only after a successful, non-empty SQL write.

This wrapper is useful for audit exports, downstream reporting feeds, and
staging data for further transformation outside TM1.

SQL safety
----------

SQL queries, delete statements, schemas, and type fragments are trusted
configuration. Bedrock escapes generated identifiers for its direct backend
routes, but it does not sanitize arbitrary caller-provided SQL.

On failure, built-in DB-API routes attempt rollback and preserve the original
error even if rollback or cursor cleanup also fails.

Backend notes
-------------

The SQLAlchemy route provides the broadest portability. Direct routes are
optimized for their named drivers. Validate generated DDL, truncate behavior,
parameter placeholders, and type mappings against the exact production
database and driver version.
