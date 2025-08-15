.. role:: python(code)
   :language: python


======================================================
`async_executor_sql_to_tm1` Parallel Processing Manual
======================================================

.. contents:: Table of Contents
   :depth: 2

------

Introduction
============

The ``async_executor_sql_to_tm1()`` function is a high-performance orchestrator designed for loading a **single, large SQL table or query result** into a TM1 cube. It achieves parallelization by "blindly paginating" the source data: it first determines the total number of rows, then divides the work into slices based on row position.

Each worker thread is assigned a unique slice (e.g., rows 0-99,999, 100,000-199,999, etc.) and executes a SQL query using ``OFFSET`` and ``FETCH`` to retrieve only its assigned data. It then runs the full ``load_sql_data_to_tm1_cube`` process on its slice.

This architecture is the correct tool for parallelizing the load of a single, large SQL source, as it parallelizes the read from the database itself.

------

How It Works: The Slicing Mechanism
===================================

The function’s parallelization strategy is based on **SQL result set pagination**. It does **not** use TM1 dimension elements for slicing. It follows a clear, three-step process:

1.  **Count Total Records**: The function first runs a ``COUNT(*)`` on the table specified in ``sql_table_for_count`` to determine the total number of records to be processed.

2.  **Calculate Slices**: Based on the ``slice_size`` parameter, it calculates the total number of parallel chunks required to process all records.

3.  **Assign Paginated Queries**: The function then iterates through the calculated number of chunks. For each chunk, it populates the ``sql_query_template`` with unique ``{offset}`` and ``{fetch}`` values.
    -   Worker 1 gets a query like: `... OFFSET 0 ROWS FETCH NEXT 100000 ROWS ONLY`
    -   Worker 2 gets a query like: `... OFFSET 100000 ROWS FETCH NEXT 100000 ROWS ONLY`

4.  **Execute in Parallel**: Each worker thread is assigned one of these unique, paginated SQL queries. It then executes the ``load_sql_data_to_tm1_cube`` function on its specific data slice.

This ensures that each worker operates on a distinct, non-overlapping slice of the SQL source data.

------

Parameter Reference
===================

Below is a complete reference for the function’s parameters.

tm1_service
    *(required)* An active `TM1Service` object for the target TM1 connection.

sql_engine
    *(required)* A pre-configured SQLAlchemy Engine object for the source database connection.

sql_query_template
    *(required, string)* This is the **workload template**. It is a SQL query string that **must** contain an ``ORDER BY`` clause for deterministic slicing. It must also contain two placeholders for the pagination logic: ``{offset}`` and ``{fetch}``.

    *Example (MS SQL)*: ``SELECT * FROM dbo.FactSales ORDER BY SaleID OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY``

sql_table_for_count
    *(required, string)* The name of the SQL table to perform a ``COUNT(*)`` on. This is used to calculate the total number of slices needed.

target_cube_name
    *(required, string)* The name of the destination cube in TM1.

slice_size
    *(optional, int; default=100000)* The number of rows each parallel worker will fetch from the database. This is a key performance tuning parameter.

data_copy_function
    *(optional, callable)* The function to be executed by each worker. Defaults to ``bedrock.load_sql_data_to_tm1_cube``.

target_clear_set_mdx_list
    *(optional, list[string])* A list of MDX set expressions defining the slice to be cleared in the target TM1 cube. This clear operation is performed **once** before any workers start.

max_workers
    *(optional, int; default=8)* The number of parallel worker threads to execute. This is the **primary performance tuning parameter**.

shared_mapping / mapping_steps
    *(optional)* The standard mapping and transformation dictionaries, which are passed through to each worker’s ``load_sql_data_to_tm1_cube`` call.

**kwargs
    *(optional)* Additional keyword arguments to be passed down to each call of the ``load_sql_data_to_tm1_cube`` function. This is the mechanism for providing function-specific parameters like ``sql_column_mapping``, ``async_write``, ``increment``, etc.

------

Example Workflow
================

This example demonstrates using the executor to load a large sales fact table from a SQL database into a TM1 cube in parallel.

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock
    from sqlalchemy import create_engine

    # 1. Define the SQL query template with ORDER BY, offset, and fetch placeholders
    # NOTE: ORDER BY is mandatory for deterministic slicing!
    sql_tmpl = """
    SELECT
        ProductID,
        RegionID,
        DateID,
        SalesValue
    FROM
        dbo.FactSales
    ORDER BY
        DateID, ProductID
    OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY
    """

    # 2. Define how to map SQL columns to TM1 dimension names
    col_map = {
        "ProductID": "Product",
        "RegionID": "Region",
        "DateID": "Period",
        "SalesValue": "Value"
    }

    # 3. Create the SQL engine
    engine = create_engine("mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server")

    # 4. Run the executor
    asyncio.run(bedrock.async_executor_sql_to_tm1(
        tm1_service=tm1_connection,
        sql_engine=engine,
        sql_query_template=sql_tmpl,
        sql_table_for_count="dbo.FactSales",
        target_cube_name="Sales",
        slice_size=250000,
        max_workers=8,

        # Pass-through kwargs for the underlying load_sql_data_to_tm1_cube function
        sql_column_mapping=col_map,
        use_blob=True,
        increment=False,
        clear_target=True,
        target_clear_set_mdx_list=["{[Version].[Actual]}"]
    ))

------

Developer Comments & Performance Tuning
=======================================

.. warning::

    **ORDER BY Clause is Mandatory**

    For the ``OFFSET``/``FETCH`` slicing to be reliable and produce a consistent, non-overlapping result, the ``sql_query_template`` **must** include a deterministic ``ORDER BY`` clause (e.g., ``ORDER BY PrimaryKey``). Without it, the database provides no guarantee on the order of rows, and you may process duplicate rows or miss others entirely.

.. note::

    **Performance Tuning**

    *   **``max_workers``**: The optimal value (typically 4-16) depends on the source SQL server's capacity for handling concurrent queries and the TM1 server's capacity for concurrent writes.
    *   **``slice_size``**: Controls the trade-off between the overhead of running many small SQL queries and the memory usage of fetching large chunks. A good starting point is between 50,000 and 250,000.

.. warning::

    **Database Compatibility**

    The ``OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY`` syntax is standard for modern versions of MS SQL Server, Oracle, and DB2. Other databases like **PostgreSQL** and **MySQL** use a different syntax: ``LIMIT {fetch} OFFSET {offset}``. You must adjust the ``sql_query_template`` accordingly for your target database.

.. note::

    **Clearing Behavior**

    The ``target_clear_set_mdx_list`` parameter, if provided, executes a **single, global clear operation** on the TM1 cube **before** any parallel workers begin loading data. It does not clear data on a per-worker basis.

.. note::

    **Metadata Caching**

    The executor is optimized to pre-cache the target cube metadata once before starting the parallel workers, avoiding redundant API calls.

------

Conclusion
==========

The ``async_executor_sql_to_tm1()`` function is a specialized and powerful tool for a common integration challenge: loading a single, large table from a relational database into TM1. By parallelizing the read from the SQL source, it can dramatically reduce load times and efficiently handle datasets containing millions or even billions of rows.