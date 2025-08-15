.. role:: python(code)
   :language: python

.. role:: json(code)
   :language: json

.. role-:: sql(code)
   :language: sql

================================================
`async_executor_tm1` Parallel Processing Manual
================================================

.. contents:: Table of Contents
   :depth: 2

------

.. _introduction:

Introduction
============

The :python:`async_executor_tm1()` function is a high-performance, versatile orchestrator for running parallel data operations that **originate from a TM1 cube**. It is the primary tool in the toolkit for scaling up large data extraction, transformation, and movement tasks.

The core principle is **slicing a large TM1 data source into smaller, independent chunks** and assigning each chunk to a separate worker thread to be processed concurrently. This architecture is ideal for maximizing throughput and dramatically reducing the execution time of large jobs.

This executor is a "meta-orchestrator" designed to wrap several different bedrock functions, including:

- :doc:`data_copy <data_copy>`: For parallel, in-cube transformations across different data slices.
- :doc:`data_copy_intercube <data_copy>`: For parallel data movement between cubes.
- :doc:`load_tm1_cube_to_csv_file <tm1_csv>`: For exporting multiple TM1 data slices to separate CSV files in parallel.

------

.. _how_it_works:

How It Works: The Slicing Mechanism
===================================

The function's parallelization strategy is based on parameterizing an MDX query. It follows a clear, three-step process to create a unique workload for each worker thread:

1.  **Generate Parameter Tuples**: The function first executes the MDX set queries provided in the :python:`param_set_mdx_list`. This generates a list of unique element combinations. For example, if you provide :python:`["{[Period].[All Months]}"]`, it will generate a list of tuples like :python:`[('202401',), ('202402',), ...]`.

2.  **Substitute into Template**: The function then iterates through this list of parameter tuples. For each tuple, it substitutes the element names into the :python:`data_mdx_template` string.

3.  **Assign Unique Work**: Each worker thread is assigned one of these newly generated, unique MDX queries. It then executes the specified :python:`data_copy_function` using its unique MDX query as the data source.

This ensures that each worker operates on a distinct, non-overlapping slice of the source data.

------

.. _parameter_reference:

Parameter Reference
===================

Below is a complete reference for the function's parameters.

tm1_service
  *(required)* An active `TM1Service` object. This connection is used for the initial setup (e.g., fetching parameter elements) and is then passed to each worker thread.

param_set_mdx_list
  *(required, list[string])* This is the **slicing definition**. It is a list of MDX set queries that define the parameters for parallelization. Each query should return a set of elements from a single dimension.

  *Example*: :python:`["{[Period].[All Months]}", "{[Version].[Actual,Budget]}"]`

data_mdx_template
  *(required, string)* This is the **workload template**. It is a string for the main MDX query that each worker will execute. It **must** contain placeholders that match the dimension names from `param_set_mdx_list`, prefixed with a ``$``.

  *Example*: :sql:`SELECT ... FROM [Sales] WHERE ([Period].[$Period], [Version].[$Version])`

data_copy_function
  *(required, callable)* The bedrock function to be executed by each worker. Valid built-in options are:
    - :python:`bedrock.data_copy`
    - :python:`bedrock.data_copy_intercube`
    - :python:`bedrock.load_tm1_cube_to_csv_file`

clear_param_templates
  *(optional, list[string])* A list of MDX set templates for clearing. For each worker, these templates are populated with the worker's specific parameters to generate a unique `target_clear_set_mdx_list`. This ensures each worker can clear its own target slice before loading.

max_workers
  *(optional, int; default=8)* The number of parallel worker threads to execute. This is the **primary performance tuning parameter**.

shared_mapping / mapping_steps
  *(optional)* The standard mapping and transformation dictionaries, which are passed through to each worker's `data_copy_function` call.

**kwargs
  *(optional)* Additional keyword arguments to be passed down to each call of the `data_copy_function`. This is the mechanism for providing function-specific parameters like :python:`target_cube_name`, :python:`target_csv_output_dir`, :python:`skip_zeros`, :python:`skip_consolidated_cells`, etc.

------

.. _example_workflow:

Example Workflow
================

This example demonstrates using the executor to copy two versions from a source cube to a target cube in parallel.

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock

    # 1. Define the slicing parameters (Actual and Budget versions)
    params = ["{[Version].['Actual', 'Budget']}"]

    # 2. Define the MDX template with a placeholder for the Version dimension
    mdx_tmpl = "SELECT {[Period].Leaves} ON 0 FROM [SourceCube] WHERE ([Version].[$Version])"

    # 3. Define clear templates that use the same parameter
    clear_tmpl = ["{[Period].Leaves}", "{[Version].[$Version]}"]

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
        use_blob=True,
        async_write=False
    ))

------

.. _developer_comments:

Developer Comments & Performance Tuning
=======================================

.. note::
   **Performance Tuning**

   The optimal number of workers depends on the TM1 server's CPU capacity. A good starting point is between 4 and 12. Increasing `max_workers` will improve performance up to the point where the TM1 server's CPU becomes saturated with concurrent MDX query executions. Monitor the TM1 server's CPU usage during execution to find the sweet spot.

   Default value is `use_blob=False` as `True` needs administrator privileges. Setting the value to `True` improves performance significantly.

.. note::
   **Slicing Strategy is Key**

   The effectiveness of the parallelization depends entirely on your slicing strategy in `param_set_mdx_list`. Good strategies create slices of roughly equal size and processing time. Slicing by a time dimension (e.g., month, quarter) is often a very effective approach.

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