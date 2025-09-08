.. role:: python(code)
   :language: python

.. role:: json(code)
   :language: json

.. role:: sql(code)
   :language: sql

================================================
TM1 / TM1 Parallel Processing Manual
================================================

.. contents:: Table of Contents
   :depth: 2

------

.. _introduction:

Introduction
============

The :python:`async_executor_tm1()` function is a high-performance, versatile orchestrator for running parallel data operations that **originate from a TM1 cube**. The core mechanism behind parallelism is data slicing, where independent chunks get assigned to separate worker threads to be processed concurrently.

This executor is a "meta-orchestrator" designed to wrap several different bedrock functions, including:

- :doc:`data_copy <data_copy>`: For parallel, in-cube transformations across different data slices.
- :doc:`data_copy_intercube <data_copy>`: For parallel data movement between cubes.
- :doc:`load_tm1_cube_to_csv_file <tm1_csv>`: For exporting multiple TM1 data slices to separate CSV files in parallel. For more information on asynchronous TM1->CSV loading, please refer to :doc:`async_csv`.

------

.. _how_it_works:

How It Works: The Slicing Mechanism
===================================

The function's parallelization strategy is based on parameterizing an MDX query. It follows a three-step process to create a unique workload for each worker thread:

1.  **Generate Parameter Tuples**: The function first executes the MDX set queries provided in the :python:`param_set_mdx_list` and generates a list of unique element combinations. For example, if you provide :python:`["{[Period].[All Months]}"]`, it will generate a list of tuples like :python:`[('202401',), ('202402',), ...]`.

2.  **Substitute into Template**: The function then iterates through this list of parameter tuples. For each tuple, it substitutes the element names into the :python:`data_mdx_template` string.

3.  **Assign Unique Work**: Each worker thread is assigned a unique MDX query, which the :python:`data_copy_function` executes.

These steps ensure that each worker operates on a distinct, non-overlapping slice of the source data.

------

.. _parameter_reference:

Parameter Reference
===================

Below is a complete reference for the function's parameters.

1. **tm1_service** *(required)*

   - An active `TM1Service` object. This connection is used for the initial setup (e.g., fetching parameter elements) and is then passed to each worker thread.

2. **param_set_mdx_list** *(required, list[string])*

   - This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.
   - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

3. **data_mdx_template** *(required, string)*

   - This is the **workload template** for the main MDX query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.
   - *Example*: :sql:`SELECT ... FROM [Sales] WHERE ([Period].[$Period], [Version].[$Version])`

4. **data_copy_function** *(required, callable)*

   - The bedrock function to be executed by each worker. Valid built-in options are:
      - :python:`bedrock.data_copy`
      - :python:`bedrock.data_copy_intercube`
      - :python:`bedrock.load_tm1_cube_to_csv_file`

5. **clear_param_templates** *(optional, list[string])*

   - A list of MDX set templates for clearing the target slice of the input cube, populated with worker-specific parameters to generate a unique `target_clear_set_mdx_list`.

6. **max_workers** *(optional, int; default=8)*

   - The number of parallel worker threads to execute.

7. **shared_mapping / mapping_steps** *(optional)*

   - The standard mapping and transformation dictionaries, which are passed through to each worker's `data_copy_function` call.

8. ****kwargs** *(optional)*

   - Additional keyword arguments to be passed down to each call of the `data_copy_function`. This is the mechanism for providing function-specific parameters like :python:`target_cube_name`, :python:`target_csv_output_dir`, :python:`skip_zeros`, :python:`skip_consolidated_cells`, etc.

------

.. _example_workflow:

Example Workflow
================

This example demonstrates using the executor to copy two versions from a source cube to a target cube in parallel.

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock

    # 1. Define the slicing parameters (Actual and Budget versions)
    params = ["{[Version].[Version].['Actual', 'Budget']}"]

    # 2. Define the MDX template with a placeholder for the Version dimension
    mdx_tmpl = "SELECT {[Period].[Period].Leaves} ON 0 FROM [SourceCube] WHERE ([Version].[Version].[$Version])"

    # 3. Define clear templates that use the same parameter
    clear_tmpl = ["{[Period].[Period].Leaves}", "{[Version].[Version].[$Version]}"]

    # 4. Run the executor
    asyncio.run(bedrock.async_executor_tm1(
        data_copy_function=bedrock.data_copy_intercube,
        tm1_service=tm1_connection,
        param_set_mdx_list=params,
        data_mdx_template=mdx_tmpl,
        clear_param_templates=clear_tmpl,
        max_workers=2,
        # Pass-through kwargs for the underlying data_copy_intercube function
        target_cube_name="TargetCube",
        skip_zeros=True,
        async_write=False,  # The executor handles the async part; the worker should be synchronous.
        use_blob=True   # Defaults to False since True needs administrator privilege. Setting true significantly improves performance.
    ))

------

.. _developer_comments:

Developer Comments & Performance Tuning
=======================================

.. note::
   **Performance Tuning**

   The optimal number of workers depends on the CPU capacity of the TM1 server. A good starting point is between 4 and 12. Increasing `max_workers` will improve performance up to the point where the TM1 server's CPU becomes saturated with concurrent MDX query executions. Monitor the TM1 server's CPU usage during execution to determine the optimal settings.

   For better performance, consider setting :python:`use_blob=True`. The default value of the parameter is :python:`False`, since :python:`True` needs administrator privileges. Setting the value to `True` improves performance significantly.

.. note::
   **Slicing Strategy is Key**

   The effectiveness of the parallelization depends greatly on your slicing strategy in :python:`param_set_mdx_list`. Good strategies create slices of roughly equal size and processing time.

.. warning::
   **Thread Safety and Connection Sharing**

   This executor passes the **same `tm1_service` object** to all worker threads. For most read-heavy operations, this is safe and efficient. However, for very long-running jobs (e.g., >20 minutes), the TM1 session can time out, and multiple threads attempting to re-login simultaneously can cause a :python:`CookieConflictError`.

.. note::
   **Metadata Caching**

   The executor is optimized to pre-cache source and target cube metadata once before starting the parallel workers. This avoids redundant API calls and improves overall performance.

------

.. _conclusion:

Conclusion
==========

The :python:`async_executor_tm1()` function is a powerful tool for scaling TM1 data operations. By understanding its slicing mechanism and performance characteristics, you can significantly reduce the runtime of large data copy, transformation, and export jobs. Its versatility allows it to be the central orchestrator for a wide variety of TM1-centric parallel workflows.