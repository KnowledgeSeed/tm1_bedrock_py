.. role:: python(code)
   :language: python

.. role:: json(code)
   :language: json

.. role:: sql(code)
   :language: sql

=================================
TM1 / SQL Data Integration Manual
=================================

.. contents:: Table of Contents
   :depth: 2

------

.. _introduction:

Introduction
============

The **TM1/SQL Integration** module serves as an interface between TM1 cubes and relational databases. Its primary goal is to **transform and copy (or move) data** between the two systems.

This module is designed with key priorities in mind:

- **Performance**: Natively supports high-speed, database-specific bulk loading methods (e.g., MS SQL's `fast_executemany`).
- **Robustness**: Automatically handles common data integration pitfalls, such as mismatched column orders, data type ambiguity, and TM1 session timeouts during long operations.
- **Flexibility**: Provides extensive control over both SQL data extraction and TM1 data writing.

The module provides two core functions:

- :python:`load_sql_data_to_tm1_cube`: For loading and transforming data **from a SQL database into a TM1 cube**.
- :python:`load_tm1_cube_to_sql_table`: For extracting, transforming, and archiving data **from a TM1 cube into a SQL database**.

------

.. _default_operations:

Default Operations
==================

**load_sql_data_to_tm1_cube (SQL -> TM1)**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


1.  **Extract from SQL**: The source data is read from a SQL table or query into a pandas DataFrame.
2.  **Normalize DataFrame**: The raw tabular data is normalized into a TM1-ready format (one value per row with dimension columns) by renaming columns and identifying the `Value` column.
3.  **Apply Mapping Transformations**: The powerful mapping engine\ :doc:`[1] <data_copy>` (`replace`, `map_and_replace`, `map_and_join`) is used to transform the data.
4.  **Redimensionalize**: Columns are added, removed, or renamed to precisely match the structure of the target TM1 cube.
5.  **Clear Target Cube**: If requested, the target slice in the TM1 cube is cleared.
6.  **Write to TM1**: The final, clean DataFrame is written to the TM1 cube, with support for high-performance modes like `async_write` and `use_blob`.

**load_tm1_cube_to_sql_table (TM1 -> SQL)**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.  **Extract from TM1**: Data is extracted from the source TM1 cube using an MDX query.
2.  **Apply Mapping Transformations**: The same mapping engine is used to transform the extracted data.
3.  **Redimensionalize**: Columns are added, removed, or renamed as needed.
4.  **Clean for SQL**: A robust data cleaning step ensures that the `Value` column has clean, unambiguous data types (handling mixed numeric/string data and TM1's locale-specific number formats).
5.  **Align Column Order**: The function automatically inspects the target SQL table and **reorders the DataFrame columns** to match the table's physical order, preventing a common class of insertion errors.
6.  **Clear Target Table**: If requested, the target SQL table is cleared using a `TRUNCATE` or custom `DELETE` statement.
7.  **Write to SQL**: The final, clean DataFrame is written to the target SQL table, with support for performance tuning via `chunksize` and `sql_insert_method`.
8.  **Clear Source Cube**: If requested, the source slice in the TM1 cube is cleared. A **TM1 reconnect** is automatically performed before this step to prevent session timeout errors.

------

.. _parameter_reference:

Parameter Reference
===================

Below is a complete reference for all function parameters, grouped by category.

.. _tm1_connection_data:

TM1 Connection & Data
~~~~~~~~~~~~~~~~~~~~~

1. **tm1_service** *(required)*

   - A valid `TM1Service` object for connecting to the TM1 instance.

2. **target_cube_name** *(string; only in `load_sql_data_to_tm1_cube`)*

   - The name of the target TM1 cube where data will be written.

3. **data_mdx** *(string; only in `load_tm1_cube_to_sql_table`)*

   - An MDX query to extract the source data from a TM1 cube.

.. _sql_source_config:

SQL Source Configuration (`load_sql_data_to_tm1_cube`)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **sql_engine** *(required)*

   - A valid `SQLAlchemy Engine` object for the source database connection.

2. **sql_query** *(optional, string)*

   - A full SQL query to execute for data extraction. Use this or `sql_table_name`.

3. **sql_table_name** *(optional, string)*

   - The name of the SQL table to extract data from.

4. **sql_column_mapping** *(optional, dict)*

   - A dictionary to rename columns from the SQL source to match TM1 dimension names. Example: :json:`{"PRODUCT_CODE": "Product"}`.

5. **sql_value_column_name** *(optional, string)*

   - The name of the column in the SQL source that contains the data values. This column will be automatically renamed to `Value`.

6. **chunksize** *(optional, int)*

   - The number of rows to read from the SQL database at a time. This is a **memory optimization** for very large source tables.

.. _sql_target_config:

SQL Target Configuration (`load_tm1_cube_to_sql_table`)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **target_table_name** *(required, string)*

   - The name of the target table in the SQL database.

2. **sql_engine** *(required)*

   - A valid `SQLAlchemy Engine` object for the target database connection.

3. **sql_dtypes** *(optional, dict)*

   - A dictionary mapping column names to `SQLAlchemy` types (e.g., :python:`{"Value": types.FLOAT, "Version": types.VARCHAR(50)}`). **Providing this is a best practice** to prevent data type inference errors.

4. **sql_insert_method** *(optional)*

   - The method for `pandas.to_sql` to use. For MS SQL, `None` is recommended to enable `fast_executemany`. For PostgreSQL, a specific callable for `COPY` is fastest.

5. **chunksize** *(optional, int)*

   - The number of rows to write to the SQL table in a single batch. This is a **memory optimization**. For best performance with high-speed methods like `fast_executemany`, this should often be `None`.

6. **clear_target** *(optional, boolean; default=False)*

   - If `True`, the **entire target SQL table is cleared once** before any workers start.

7. **sql_delete_statement** *(optional, string)*

   - A specific SQL statement to use for clearing the target table.
   - If :python:`clear_target=True` and :python:`sql_delete_statement=None`, it defaults to a truncate statement with MS SQL syntax. To make sure seamless clearing use a custom statement.


.. _shared_logic_params:

Shared Logic (Mapping, Transformation, Clearing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The SQL integration functions leverage the same powerful transformation engine as the :doc:`data_copy <data_copy>` functions. The following parameters work identically. For detailed examples of the mapping methods, please refer to that manual.

- **shared_mapping** and **mapping_steps**: For applying `replace`, `map_and_replace`, and `map_and_join` transformations.
- **source_dim_mapping**, **related_dimensions**, **target_dim_mapping**: For redimensionalizing the DataFrame to match the target structure.
- **value_function**: For applying a custom function to the `Value` column.
- **clear_target** / **clear_source**: Booleans to enable clearing of the target (in TM1 or SQL) or source (in TM1 or SQL).
- **target_clear_set_mdx_list** / **sql_delete_statement**: Statements to define the slice to be cleared.

------

.. _example_workflow:

Example Workflow
================

**Example 1: Loading from a SQL Table into TM1**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service
    from sqlalchemy import create_engine

    # Create engine
    engine = create_engine("mssql+pyodbc://...")

    # Define how to map SQL columns to TM1 dimension names
    column_map = {
        'SourceVersion': 'Version',
        'SourcePeriod': 'Period',
        'SalesAmount': 'Value'
    }

    # Call the function to load data from a SQL table
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        bedrock.load_sql_data_to_tm1_cube(
            tm1_service=tm1,
            sql_engine=engine,
            target_cube_name="Sales",
            sql_table_name="dbo.FactSales",
            sql_column_mapping=column_map,
            target_clear_set_mdx_list=["{[Version].[Version].[Actual]}"],
            clear_target=True,
            async_write=True
        )

**Example 2: Exporting from TM1 to a SQL Table**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service
    from sqlalchemy import types, create_engine

    # Create engine
    engine = create_engine("mssql+pyodbc://...")

    # Define the explicit data types for the target SQL table
    sql_types = {
        'Version': types.VARCHAR(50),
        'Period': types.VARCHAR(50),
        'Value': types.FLOAT
    }

    # Export a slice of a TM1 cube to a SQL table
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        bedrock.load_tm1_cube_to_sql_table(
            tm1_service=tm1,
            sql_engine=engine,
            target_table_name="SalesArchive",
            data_mdx="SELECT {[Version].[Actual]} ON 0 FROM [Sales]",
            related_dimensions={"Value": "SalesAmount"},
            sql_dtypes=sql_types,
            sql_delete_statement="TRUNCATE TABLE [SalesArchive]",
            clear_target=True,
            skip_zeros=True
        )

------

.. _developer_comments:

Developer Comments
==================

.. warning::
   **Tested Databases**:
   For the 1.0.0 release, this functionality has been explicitly tested against **MS SQL Server** and **PostgreSQL**. While the toolkit is designed for portability using SQLAlchemy, behavior with other database backends (Oracle, MySQL, etc.) has not been verified in this version.

.. warning::
   **Column Order Matters**: The `load_tm1_cube_to_sql_table` function automatically inspects the target SQL table and reorders the DataFrame columns to match. This is a critical safety feature that prevents `COUNT field incorrect` errors and silent data corruption.

.. warning::
   **TM1 Session Timeouts**: When exporting large datasets from TM1 to SQL, the SQL write can be a long operation. The function will automatically and proactively call `tm1_service.re_connect()` before clearing the cube to prevent a `CookieConflictError` caused by an expired TM1 session.

.. note::
   **Data Type Ambiguity**: It is best practice to provide the `sql_dtypes` parameter when writing to SQL. This removes all guesswork from the database driver and is the most robust way to prevent data type conversion errors (e.g., `nvarchar to float`).

.. note::
   **Performance Tuning**: For the fastest possible writes to MS SQL Server, create your `SQLAlchemy Engine` with `use_fast_executemany=True` and call the function with `chunksize=None`. `chunksize` is a memory optimization, not a performance one, and can interfere with high-speed bulk insert methods.

------

.. _conclusion:

Conclusion
==========

This manual describes the core functionality of the **TM1/SQL Integration** module. It details how to reliably:

1.  **Read** data from either a TM1 cube or a SQL database.
2.  **Transform** the data using the toolkit's consistent and powerful mapping engine.
3.  **Write** the final, clean, and correctly structured data to the other system with robust error handling and performance tuning.

By providing a flexible and high-performance bridge between TM1 and relational databases, this module empowers developers to build sophisticated data warehousing, archiving, and integration workflows.