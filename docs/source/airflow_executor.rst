.. role:: python(code)
   :language: python

.. role:: sql(code)
   :language: sql

============================================================
Airflow Task Groups for TM1 Bedrock Pipelines
============================================================

The ``TM1_bedrock_py.airflow_executor.async_executor`` module exposes a collection of
`Apache Airflow <https://airflow.apache.org/>`_ task-group factories. Each factory
wraps one of the synchronous Bedrock workflows and turns it into a DAG-friendly
building block that Airflow can schedule in parallel. This page documents every task
group, its parameters, and the shared orchestration pattern.

.. contents::
   :local:
   :depth: 2

Overview
========

Every task group follows the same four-stage workflow:

1. **Prepare metadata** – shared mapping dataframes are built,
   target-cube metadata is cached, and SQL/CSV resources are enumerated.
2. **Clear the target once** – cubes or tables are wiped before fan-out so each
   slice writes into a clean space.
3. **Generate slice parameters** – the same slicing helpers used in
   :doc:`async_executor` expand the :python:`param_set_mdx_list` into a workload plan.
4. **Delegate parallel execution to Airflow** – each slice becomes an individual task
   managed by Airflow’s scheduler, which provides pooling, retries, backfills, and
   timeouts without relying on Python threads.

.. figure:: task_group.png
   :alt: Task group flow
   :width: 70%
   :align: center

   High-level flow example: [metadata prep, target clear, slicing] → Airflow fan-out.


All task groups share the :python:`bedrock_params` dictionary. Only the keys
referenced below are mandatory; every other Bedrock argument can be passed through the
dictionary and is forwarded as-is to the synchronous helper.

Task Groups
===========

TM1 ↔ TM1 Slice Loader
----------------------

.. py:function:: tm1_dynamic_executor_task_group(tm1_connection, bedrock_params, dry_run=False, logging_level="INFO")

   Moves data between TM1 cubes using :python:`data_copy_intercube` with Airflow-level
   parallelisation.

   **Key bedrock_params**

   1. **data_mdx_template** *(required, string)*

      - This is the **workload template** for the main MDX query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.
      - *Example*: :sql:`SELECT ... FROM [Sales] WHERE ([Period].[$Period], [Version].[$Version])`

   2. **param_set_mdx_list** *(required, list[string])*

      - This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

   3. **target_cube_name** *(required, string)*

      - The name of the target cube to clear before writing.

   4. **target_clear_set_mdx_list** *(required, list[string])*

      - A list of set MDX lists for clearing the target slices of the input cube.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[ForeCast]}"]`

   5. Optional :python:`mapping_steps`, :python:`shared_mapping`, :python:`use_mixed_datatypes`

SQL ↔ TM1 Loaders
-----------------

.. py:function:: sql_to_tm1_dynamic_executor_task_group(tm1_connection, sql_connection, bedrock_params, dry_run=False, logging_level="INFO")

   Streams relational data into TM1. The SQL connection is resolved through
   ``BaseHook`` and each slice executes its own parametrised SQL template.

   **Key bedrock_params**

   1. **sql_query_template** *(required, string)*

      - This is the **workload template** for the main SQL query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.
      - *Example*: :sql:`SELECT ... FROM sales s WHERE s.period = '$Period'`

   2. **param_set_mdx_list** *(required, list[string])*

      - This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

   3. **target_cube_name** *(required, string)*

      - The name of the target cube to clear before writing.

   4. **target_clear_set_mdx_list** *(required, list[string])*

      - A list of set MDX lists for clearing the target slices of the input cube.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

   5. Optional :python:`use_mixed_datatypes`, mapping payloads


.. py:function:: tm1_to_sql_dynamic_executor_task_group(tm1_connection, sql_connection, bedrock_params, dry_run=False, logging_level="INFO")

   Reads TM1 slices and writes them to a SQL table. Before fan-out, the target table
   is cleared via :python:`clear_sql_table_task`.

   **Key bedrock_params**

   1. **data_mdx_template** *(required, string)*

      - This is the **workload template** for the main MDX query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.
      - *Example*: :sql:`SELECT ... FROM [Sales] WHERE ([Period].[$Period], [Version].[$Version])`

   2. **param_set_mdx_list** *(required, list[string])*

      - This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

   3. **target_table_name** *(required, string)*

      - The name of the target table to write.

   4. **sql_delete_statement** *(optional, string)*

      - Custom SQL delete statement. If missing, then the table is truncated based on :python:`target_table_name`.

   - Optional :python:`related_dimensions`, :python:`decimal`, :python:`sql_schema`, mapping payloads

CSV ↔ TM1 Loaders
-----------------

.. py:function:: csv_to_tm1_dynamic_executor_task_group(tm1_connection, bedrock_params, dry_run=False, logging_level="INFO")

   Discovers ``*.csv`` files below :python:`source_directory` and processes each file
   as one Airflow task. Target cube clears and metadata caching mirror the TM1 loaders.

   **Key bedrock_params**

   1. **target_cube_name** *(required, string)*

      - The name of the destination cube in TM1.

   2. **source_directory** *(required, string)*

      - The full path to the directory containing the source CSV files to be loaded.

   3. **target_clear_set_mdx_list** *(required, list[string])*

      - A list of set MDX lists for clearing the target slices of the input cube.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

   4. Optional :python:`use_mixed_datatypes`, :python:`decimal`, :python:`delimiter`,  mapping payloads

.. py:function:: tm1_to_csv_dynamic_executor_task_group(tm1_connection, bedrock_params, dry_run=False, logging_level="INFO")

   Exports TM1 slices to CSV files using :python:`param_set_mdx_list` and the
   :python:`target_csv_output_dir`.

   **Key bedrock_params**

   1. **data_mdx_template** *(required, string)*

      - This is the **workload template** for the main MDX query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.
      - *Example*: :sql:`SELECT ... FROM [Sales] WHERE ([Period].[$Period], [Version].[$Version])`

   2. **param_set_mdx_list** *(required, list[string])*

      - This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.
      - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

   3. **target_csv_output_dir** *(required, string)*

      - The full path to the directory containing the source CSV files to be loaded.

   4. Optional :python:`decimal`, :python:`delimiter`,  mapping payloads


Multi-cube Helper
-----------------

.. py:function:: copy_cube_data_on_elements(tm1_connection, cube_names, unified_bedrock_params, logging_level="INFO")

   Convenience wrapper that expands :python:`unified_bedrock_params` per cube and
   spawns individual :python:`tm1_dynamic_executor_task_group` instances (via
   :py:meth:`TaskGroup.override`) with unique :python:`group_id` values.

Example DAG Snippet
===================

.. code-block:: python

   from airflow import DAG
   from datetime import datetime
   from TM1_bedrock_py.airflow_executor.async_executor import (
       sql_to_tm1_dynamic_executor_task_group,
   )

   with DAG(
       dag_id="sql_to_tm1_sales",
       start_date=datetime(2024, 1, 1),
       schedule="0 2 * * *",
       catchup=False,
   ) as dag:
       sql_to_tm1_dynamic_executor_task_group(
           tm1_connection="tm1_prod",
           sql_connection="warehouse",
           logging_level="INFO",
           bedrock_params={
               "target_cube_name": "Sales",
               "param_set_mdx_list": [
                   "{[Period].[Period].Children}",
                   "{[Version].[Version].['Actual','Budget']}",
               ],
               "sql_query_template": "SELECT * FROM fact_sales WHERE period = '$Period'",
               "target_clear_set_mdx_list": ["{[Period].[$Period]}"],
               "mapping_steps": [...],
               "shared_mapping": {...},
           },
       )

Additional Notes
================

- Every task group honours :python:`dry_run=True`, which suppresses TM1/SQL writes and
  simply logs the slices that would execute.
- Because Airflow manages concurrency, adjust pool/priority settings on the DAG or
  tasks instead of altering Python thread counts.
- For detailed information about the slice generation and mapping payloads referenced
  here, consult :doc:`async_executor`, :doc:`async_sql`, and :doc:`async_csv`.

.. warning::

    **Tested Databases**

    For the 1.1.1 release, this functionality has been explicitly tested against **MS SQL Server** and **PostgreSQL**. Behavior with other database backends is yet to be verified.
