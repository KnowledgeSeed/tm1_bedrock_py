Airflow Integration
===================

Install the optional dependencies:

.. code-block:: bash

   pip install "tm1-bedrock-py[airflow]"

The Airflow package is imported lazily, so core Bedrock workflows remain usable
without Airflow installed.

Task groups
-----------

``tm1_dynamic_executor_task_group``
   TM1-sliced ``data_copy_intercube`` workflow.

``sql_to_tm1_dynamic_executor_task_group``
   Parameterized SQL-to-TM1 workflow.

``tm1_to_sql_dynamic_executor_task_group``
   Parameterized TM1-to-SQL workflow.

``csv_to_tm1_dynamic_executor_task_group``
   One mapped Airflow task per discovered CSV file.

``tm1_to_csv_dynamic_executor_task_group``
   One mapped Airflow task per TM1 parameter slice.

``copy_cube_data_on_elements``
   Convenience task group that expands a unified configuration across cubes.

Each task group accepts connection identifiers and a ``bedrock_params``
dictionary. The dictionary contains the same parameters documented for the
underlying synchronous Bedrock function, plus slicing values such as
``param_set_mdx_list`` and the relevant query template.

Example
-------

.. code-block:: python

   from airflow import DAG
   from datetime import datetime
   from TM1_bedrock_py.airflow_executor.async_executor import (
       sql_to_tm1_dynamic_executor_task_group,
   )

   with DAG(
       dag_id="sales_sql_to_tm1",
       start_date=datetime(2026, 1, 1),
       schedule="0 2 * * *",
       catchup=False,
   ):
       sql_to_tm1_dynamic_executor_task_group(
           tm1_connection="tm1_prod",
           sql_connection="warehouse",
           bedrock_params={
               "target_cube_name": "Sales",
               "param_set_mdx_list": [
                   "{[Period].[Period].[2026-01], [Period].[Period].[2026-02]}",
               ],
               "sql_query_template": (
                   "SELECT Version, Period, Product, Amount AS Value "
                   "FROM FactSales WHERE Period = '$Period'"
               ),
               "target_clear_set_mdx_list": [
                   "{[Version].[Version].[Actual]}",
               ],
           },
       )

Use Airflow pools, retries, timeouts, and task concurrency controls for
production scheduling. ``dry_run=True`` suppresses external writes in task
groups that support it.

Connection and provider versions are constrained by the optional dependencies
in ``pyproject.toml``. Validate the exact Airflow and provider combination used
by the deployment.
