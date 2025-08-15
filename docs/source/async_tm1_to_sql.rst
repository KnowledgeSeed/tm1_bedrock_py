.. role:: python(code)
   :language: python


======================================================
`async_executor_tm1_to_sql` Parallel Processing Manual
======================================================

.. contents:: Table of Contents
   :depth: 2

------

Introduction
============

The ``async_executor_tm1_to_sql()`` function is a high-performance orchestrator designed to export large, sliceable datasets from a TM1 cube to a SQL table. It works by defining a set of parameters (e.g., all months in a year) and then creating a separate, parallel worker thread for each parameter. Each worker executes a unique MDX query to extract its assigned data slice, transforms it, and loads it into the target SQL table.

This architecture is ideal for maximizing throughput by parallelizing the data extraction from TM1 and the data insertion into SQL.

This executor is a "meta-orchestrator" designed to wrap the ``load_tm1_cube_to_sql_table`` bedrock function, distributing its execution across multiple cores and threads.

------

How It Works: The Slicing Mechanism
===================================

The function’s parallelization strategy is based on parameterizing an MDX query. It follows a clear, three-step process to create a unique workload for each worker thread:

1.  **Generate Parameter Tuples**: The function first executes the MDX set queries provided in the ``param_set_mdx_list``. This generates a list of unique element combinations. For example, if you provide ``["{[Period].[All Months]}"]``, it will generate a list of tuples like ``[('202401',), ('202402',), ...]``.

2.  **Substitute into Template**: The function then iterates through this list of parameter tuples. For each tuple, it substitutes the element names into the ``data_mdx_template`` string.

3.  **Assign Unique Work**: Each worker thread is assigned one of these newly generated, unique MDX queries. It then executes the specified ``data_copy_function`` (defaulting to ``load_tm1_cube_to_sql_table``) using its unique MDX query as the data source.

This ensures that each worker operates on a distinct, non-overlapping slice of the source data.

------

Parameter Reference
===================

Below is a complete reference for the function’s parameters.

tm1_service
    *(required)* An active `TM1Service` object. This connection is used for initial setup (e.g., fetching parameter elements) and is then passed to each worker thread.

sql_engine
    *(required)* A pre-configured SQLAlchemy Engine object for the target database connection.

target_table_name
    *(required, string)* The name of the destination table in the SQL database.

param_set_mdx_list
    *(required, list[string])* This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.

    *Example*: ``["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]``

data_mdx_template
    *(required, string)* This is the **workload template**. It is a string for the main MDX query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.

    *Example*: ``SELECT ... FROM [Sales] WHERE ([Period].[$Period], [Version].[$Version])``

data_copy_function
    *(optional, callable)* The bedrock function to be executed by each worker. Defaults to ``bedrock.load_tm1_cube_to_sql_table``.

clear_target
    *(optional, boolean; default=False)* If `True`, the **entire target SQL table is cleared once** with a single TRUNCATE or DELETE operation **before any workers start**.

sql_delete_statement
    *(optional, string)* A specific SQL statement to use for clearing the target table if `clear_target` is True. If not provided, a `TRUNCATE` command is used.

max_workers
    *(optional, int; default=8)* The number of parallel worker threads to execute. This is the **primary performance tuning parameter**.

shared_mapping / mapping_steps
    *(optional)* The standard mapping and transformation dictionaries, which are passed through to each worker’s ``load_tm1_cube_to_sql_table`` call.

**kwargs
    *(optional)* Additional keyword arguments to be passed down to each call of the ``load_tm1_cube_to_sql_table`` function. This is the mechanism for providing function-specific parameters like ``sql_dtypes``, ``chunksize``, ``skip_zeros``, ``sql_insert_method``, etc.

------

Example Workflow
================

This example demonstrates using the executor to export two versions of data from a TM1 cube to a SQL table in parallel.

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock
    from sqlalchemy import types, create_engine

    # 1. Define the slicing parameters (Actual and Budget versions)
    params = ["[Version].['Actual', 'Budget']"]

    # 2. Define the MDX template with a placeholder for the Version dimension
    mdx_tmpl = "SELECT {[Period].Leaves} ON 0 FROM [SourceCube] WHERE ([Version].[$Version])"

    # 3. Define the explicit data types for the target SQL table
    sql_types = {
        'Version': types.VARCHAR(50),
        'Period': types.VARCHAR(50),
        'Value': types.FLOAT
    }

    # 4. Create a high-performance SQL engine (example for MS SQL)
    # Ensure your connection string and driver are correct
    engine = create_engine("mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

    # 5. Run the executor
    asyncio.run(bedrock.async_executor_tm1_to_sql(
        tm1_service=tm1_connection,
        sql_engine=engine,
        target_table_name="SalesArchive",
        param_set_mdx_list=params,
        data_mdx_template=mdx_tmpl,
        max_workers=2,
        clear_target=True,

        # Pass-through kwargs for the underlying load_tm1_cube_to_sql_table function
        sql_dtypes=sql_types,
        skip_zeros=True,
        skip_consolidated_cells=True
    ))

------

Developer Comments & Performance Tuning
=======================================

.. note::

    **Performance Tuning**

    *   **``max_workers``**: The most critical parameter. The optimal value (typically 4-12) depends on the source TM1 server's CPU capacity and the SQL server's ability to handle concurrent connections and writes.
    *   **Slicing Strategy**: The effectiveness of the parallelization depends entirely on your slicing strategy in ``param_set_mdx_list``. Good strategies create slices of roughly equal size. Slicing by a time dimension (e.g., month) is often effective.
    *   **SQL Insertion Method**: For maximum performance, pass the necessary ``**kwargs`` to enable high-speed writing for your target database. For MS SQL Server, this means creating the ``sql_engine`` with ``use_fast_executemany=True``. For PostgreSQL, this may involve a custom callable for the `COPY` command.

.. warning::

    **Tested Databases**

    For the |release| release, this functionality has been explicitly tested against **MS SQL Server** and **PostgreSQL**. While the toolkit is designed for portability using SQLAlchemy, behavior with other database backends has not been verified.

.. note::

    **Clearing Behavior**

    The ``clear_target=True`` parameter executes a **single** `TRUNCATE` or `DELETE` operation on the entire target table **before** any parallel workers begin their tasks. It does not clear data on a per-worker basis.

.. note::

    **Metadata Caching**

    The executor is optimized to pre-cache source cube metadata once before starting the parallel workers. This avoids redundant API calls and improves overall performance.

------

Conclusion
==========

The ``async_executor_tm1_to_sql()`` function is a powerful tool for scaling up TM1 data export and archiving processes. By understanding its slicing mechanism and the importance of tuning both the number of workers and the SQL insertion method, you can dramatically reduce the runtime of large data movement jobs, turning hours of work into minutes.