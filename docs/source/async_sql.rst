.. role:: python(code)
   :language: python

.. role:: sql(code)
   :language: sql

.. _async_tm1_sql:

==================================================
TM1 / SQL Asynchronous Parallel Processing Manual
==================================================

.. contents:: Table of Contents
   :local:
   :depth: 2

------

.. _introduction:

Introduction
============

The **mission** of `tm1_bedrock_py`’s **Asynchronous TM1/SQL Integration** module is to provide a high-performance, robust, and scalable bridge between TM1 cubes and relational databases.

This module is designed for large-scale data operations where performance is critical. It achieves this by slicing large datasets into smaller, independent chunks and processing them concurrently across multiple worker threads.

The module provides two core, specialized orchestrators:

*   ``async_executor_tm1_to_sql()``: For exporting and transforming data **from a TM1 cube into a SQL database** in parallel. It slices the work based on an input MDX query template.

*   ``async_executor_sql_to_tm1()``: For loading and transforming data **from a single, large SQL table into a TM1 cube** in parallel. It slices the data by paginating the SQL query itself.

------

.. _slicing_mechanism:

Slicing Mechanisms: How Parallelization Works
==============================================

The two executors use fundamentally different strategies to parallelize their workloads. Understanding this distinction is key for optimizing performance.

.. _slicing_tm1_to_sql:

async_executor_tm1_to_sql (TM1 Parameter Slicing)
-------------------------------------------------

This function’s parallelization strategy is based on **parameterizing an MDX query**.

1.  **Generate Parameter Tuples**: It executes the MDX set queries in ``param_set_mdx_list`` to generate a list of unique element combinations (e.g., `[('202401',), ('202402',)]`).
2.  **Substitute into Template**: It iterates through these tuples, substituting the element names into the ``data_mdx_template`` string to create a unique MDX query for each worker.
3.  **Assign Unique Work**: Each worker thread is assigned one of these unique MDX queries and runs the full ``load_tm1_cube_to_sql_table`` process on that data slice.

This approach is ideal when the source data can be logically partitioned by one or more TM1 dimensions (like Period, Version, or Region).

.. _slicing_sql_to_tm1:

async_executor_sql_to_tm1 (SQL Pagination Slicing)
--------------------------------------------------

This function’s parallelization strategy is based on **SQL result set pagination**. It does **not** use TM1 dimension elements for slicing.

1.  **Count Total Records**: It runs a ``COUNT(*)`` on the source SQL table to determine the total number of records.
2.  **Calculate Slices**: Based on the ``slice_size``, it calculates the number of chunks needed to process all records.
3.  **Assign Paginated Queries**: It populates the ``sql_query_template`` with unique ``{offset}`` and ``{fetch}`` values for each worker, effectively giving each worker a "page" of the total result set.
4.  **Execute in Parallel**: Each worker thread executes its unique, paginated SQL query and runs the full ``load_sql_data_to_tm1_cube`` process on its data slice.

This approach is the correct choice for loading a single, large SQL table that cannot be easily partitioned before extraction.

------

.. _function_reference:

Function Reference
==================

.. _function_reference_shared_logic:

Shared Logic
------------

1. **tm1_service** *(required)*

   - An active `TM1Service` object.

2. **sql_engine** *(required)*

   - A pre-configured SQLAlchemy Engine object for the source/target database.

3. **max_workers** *(optional, int; default=8)*

   - The number of parallel worker threads.

4. **shared_mapping / mapping_steps** *(optional)*

   - Standard mapping and transformation dictionaries passed to each worker.

5. ****kwargs** *(optional)*

   - Additional keyword arguments passed down to each ``load_tm1_cube_to_sql_table`` / ``load_sql_data_to_tm1_cube`` call (e.g., ``sql_dtypes``, ``chunksize``, ``skip_zeros``).

.. _function_reference_tm1_to_sql:

async_executor_tm1_to_sql (TM1 -> SQL)
--------------------------------------

This function is a high-performance orchestrator designed to export large, sliceable datasets from a TM1 cube to a SQL table.

.. _parameter_reference_tm1_to_sql:

Parameter Reference
~~~~~~~~~~~~~~~~~~~

1. **target_table_name** *(required, string)*

   - The name of the destination table in the SQL database.

2. **param_set_mdx_list** *(required, list[string])*

   - The **slicing definition**. A list of MDX set queries that define the parameters for parallelization.

3. **data_mdx_template** *(required, string)*

   - The **workload template**. An MDX query string with `$`-prefixed placeholders matching the dimensions from `param_set_mdx_list`.

4. **data_copy_function** *(optional, callable)*

   - The function to be executed by each worker. Defaults to ``bedrock.load_tm1_cube_to_sql_table``.

5. **clear_target** *(optional, boolean; default=False)*

   - If `True`, the **entire target SQL table is cleared once** before any workers start.

6. **sql_delete_statement** *(optional, string)*

   - A specific SQL statement to use for clearing the target table.
   - If :python:`clear_target=True` and :python:`sql_delete_statement=None`, it defaults to a truncate statement with MS SQL syntax. To make sure seamless clearing use a custom statement.


.. _example_workflow_tm1_to_sql:

Example Workflow
~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service
    from sqlalchemy import types, create_engine

    # 1. Define the slicing parameters (Actual and Budget versions)
    params = ["[Version].[Version].['Actual', 'Budget']"]

    # 2. Define the MDX template with a placeholder
    mdx_tmpl = "SELECT {[Period].[Period].Leaves} ON 0 FROM [SourceCube] WHERE ([Version].[Version].[$Version])"

    # 3. Define the explicit data types for the target SQL table
    sql_types = {
        'Version': types.VARCHAR(50), 'Period': types.VARCHAR(50), 'Value': types.FLOAT
    }

    # 4. Map dimension names of the TM1 cube to match the the names of the SQL table columns
    related_dimensions={
        "Version": "SalesVersion",
        "Value": "SalesAmount"
    }

    # 5. Create the SQL engine
    engine = create_engine("mssql+pyodbc://...", fast_executemany=True)

    # 6. Run the executor
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        asyncio.run(bedrock.async_executor_tm1_to_sql(
            tm1_service=tm1,
            sql_engine=engine,
            target_table_name="SalesArchive",
            related_dimensions=related_dimensions,
            param_set_mdx_list=params,
            data_mdx_template=mdx_tmpl,
            max_workers=2,
            sql_dtypes=sql_types,
            skip_zeros=True,
            async_write=False, # The executor handles the async part; the worker should be synchronous.
            use_blob=True,   # Defaults to False since True needs administrator privilege. Setting true significantly improves performance.

            # Clearing the SQL table defaults to a truncate statement with MS SQL syntax, to make sure seamless clearing use a custom statement.
            sql_delete_statement="TRUNCATE TABLE [SalesArchive]",
            clear_target=True
        ))


.. _function_reference_sql_to_tm1:

async_executor_sql_to_tm1 (SQL -> TM1)
--------------------------------------

This function is a high-performance orchestrator for loading a single, large SQL table or query result into a TM1 cube.

.. _parameter_reference_sql_to_tm1:

Parameter Reference
~~~~~~~~~~~~~~~~~~~

1. **sql_query_template** *(required, string)*

   - The **workload template**. A SQL query string that **must** contain an ``ORDER BY`` clause and two placeholders: ``{offset}`` and ``{fetch}``.

2. **sql_table_for_count** *(required, string)*

   - The name of the SQL table to perform a ``COUNT(*)`` on to calculate the total number of slices.

3. **target_cube_name** *(required, string)*

   - The name of the destination cube in TM1.

4. **slice_size** *(optional, int; default=100000)*

   - The number of rows each worker will fetch from the database.

5. **data_copy_function** *(optional, callable)*

   - The function to be executed by each worker. Defaults to ``bedrock.load_sql_data_to_tm1_cube``.

6. **target_clear_set_mdx_list** *(optional, list[string])*

   - A list of MDX set expressions defining the slice to be cleared in the target TM1 cube **once** before any workers start.

.. _example_workflow_sql_to_tm1:

Example Workflow
~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service
    from sqlalchemy import create_engine

    # 1. Define the SQL query template with ORDER BY and pagination placeholders
    sql_tmpl = """
        SELECT * FROM dbo.FactSales
        ORDER BY SaleID
        OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY
    """

    # 2. Set the name of the Value column to match the SQL table
    sql_value_column_name = "SalesValue"

    # 3. Create the SQL engine
    engine = create_engine("mssql+pyodbc://...")

    # 4. Run the executor
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        asyncio.run(bedrock.async_executor_sql_to_tm1(
            tm1_service=tm1,
            sql_engine=engine,
            sql_query_template=sql_tmpl,
            sql_table_for_count="dbo.FactSales",
            target_cube_name="Sales",
            slice_size=250000,
            max_workers=8,
            sql_value_column_name=sql_value_column_name,
            clear_target=True,
            target_clear_set_mdx_list=["{[Version].[Version].[Actual]}"],
            use_blob=True,   # Defaults to False since True needs administrator privilege. Setting true significantly improves performance.
            async_write=False # The executor handles the async part; the worker should be synchronous.
        ))

------

.. _developer_comments_async_tm1_sql:

Developer Comments & Performance Tuning
=======================================

.. warning::

    **ORDER BY Clause is Mandatory (for ``async_executor_sql_to_tm1``)**

    For the ``OFFSET``/``FETCH`` slicing to be reliable, the ``sql_query_template`` **must** include a deterministic ``ORDER BY`` clause. Without it, you may process duplicate rows or miss others entirely.

.. warning::

    **Database Compatibility (for ``async_executor_sql_to_tm1``)**

    The ``OFFSET ... FETCH`` syntax is standard for MS SQL Server, Oracle, and DB2. Other databases like **PostgreSQL** and **MySQL** use ``LIMIT {fetch} OFFSET {offset}``. Adjust the ``sql_query_template`` accordingly.

.. warning::

    **Tested Databases**

    For the 1.0.0 release, this functionality has been explicitly tested against **MS SQL Server** and **PostgreSQL**. Behavior with other database backends is yet to be verified.

.. note::

    **Performance Tuning**

    *   **``max_workers``**: The most critical parameter. The optimal value depends on the source server's (TM1 or SQL) ability to handle concurrent reads and the target server's ability to handle concurrent writes.
    *   **Slicing Strategy**: For TM1-to-SQL, choose slicing dimensions in ``param_set_mdx_list`` that create balanced workloads. For SQL-to-TM1, tune the ``slice_size`` to balance query overhead and memory usage.
    *   **High-Speed Methods**: Always use performance-enhancing features when possible, such as creating the ``sql_engine`` with `fast_executemany=True` for MS SQL and passing `use_blob=True` when writing to TM1.

.. note::

    **Clearing Behavior**

    In both executors, the ``clear_target`` functionality executes a **single, global clear operation** on the entire target (SQL table or TM1 cube slice) **before** any parallel workers begin their tasks.

------

.. _conclusion_async_tm1_sql:

Conclusion
==========

The asynchronous SQL executors provide a powerful, scalable solution for integrating TM1 with relational databases. By choosing the correct executor for your data flow direction and understanding its specific slicing mechanism, you can build highly efficient and parallelized ETL processes capable of handling massive data volumes.
