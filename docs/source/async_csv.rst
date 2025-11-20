.. _tm1_csv_async_manual:

=================================================
TM1 / CSV Asynchronous Parallel Processing Manual
=================================================

.. contents:: Table of Contents
   :depth: 2

------

.. _introduction:

Introduction
============

The **mission** of `tm1_bedrock_py`’s **Asynchronous TM1/CSV Integration** module is to provide a high-performance, scalable solution for moving data between TM1 cubes and flat files. Its primary goal is to **read data from one system in parallel**, apply transformations using the toolkit's mapping engine, and then **write the results to the other system concurrently**.

This module is designed for large-scale, file-based data operations, such as nightly loads, data archiving, or integration with external systems that use CSV as an interchange format.

The module provides two core, specialized orchestrators:

*   ``async_executor_tm1()`` with ``load_tm1_cube_to_csv_file``: For exporting and transforming data **from a TM1 cube into multiple CSV files** in parallel. It slices the work based on TM1 dimension elements.

*   ``async_executor_csv_to_tm1()``: For loading and transforming data **from multiple CSV files into a TM1 cube** in parallel. It slices the work by pairing each file with a specific target destination in the cube.

------

Slicing Mechanisms: How Parallelization Works
==============================================

The two executors use different strategies to parallelize their workloads, tailored to their specific data flow direction.

async_executor_tm1 (for TM1 -> CSV)
-----------------------------------

This function’s parallelization strategy is based on **parameterizing an MDX query**.

1.  **Generate Parameter Tuples**: It executes the MDX set queries in ``param_set_mdx_list`` to generate a list of unique element combinations (e.g., `[('202401',), ('202402',)]`).
2.  **Substitute into Template**: It iterates through these tuples, substituting the element names into the ``data_mdx_template`` string to create a unique MDX query for each worker.
3.  **Assign Unique Work**: Each worker thread is assigned one of these unique MDX queries. It then executes the ``load_tm1_cube_to_csv_file`` function, writing the results of its specific data slice to a uniquely named CSV file.

This approach is ideal for archiving or exporting a large cube by breaking it down into logical, dimension-based chunks (e.g., one file per month).

async_executor_csv_to_tm1 (for CSV -> TM1)
------------------------------------------

This function’s parallelization strategy is based on **pairing source files with target parameters**.

1.  **Discover Source Files**: The executor first scans the ``source_directory`` and creates a list of all `.csv` files it finds.
2.  **Generate Parameter Tuples**: Concurrently, it executes the MDX set queries in ``param_set_mdx_list`` to generate a list of TM1 element combinations that define the target slices in the cube.
3.  **Pair Files with Parameters**: The function then **zips** the list of CSV files and the list of parameter tuples together. The first file is paired with the first parameter set, the second with the second, and so on.
4.  **Assign Unique Work**: Each worker thread is assigned one file-parameter pair. It then executes the ``load_csv_data_to_tm1_cube`` function, loading the data from its assigned file into the specific TM1 slice defined by its assigned parameters.

This approach is perfect for a nightly load process where multiple data files (e.g., one per region) need to be loaded into their corresponding areas in a TM1 cube.

------

Function Reference
==================

Shared Logic
------------

1. **tm1_service** *(required)*

   - An active `TM1Service` object.

2. **max_workers** *(optional, int; default=8)*

   - The number of parallel worker threads.

3. **shared_mapping / mapping_steps** *(optional)*

   - Standard mapping and transformation dictionaries passed to each worker.

async_executor_tm1 (for TM1 -> CSV)
-----------------------------------

This function orchestrates the parallel export of multiple TM1 data slices to individual CSV files.

Parameter Reference
~~~~~~~~~~~~~~~~~~~

1. **param_set_mdx_list** *(required, list[string])*

   - The **slicing definition**. A list of MDX set queries that define the parameters for parallelization.

2. **data_mdx_template** *(required, string)*

   - The **workload template**. An MDX query string with `$`-prefixed placeholders matching the dimensions from `param_set_mdx_list`.

3. **data_copy_function** *(required, callable)*

   - Must be set to ``bedrock.load_tm1_cube_to_csv_file``.

4. ****kwargs** *(optional)*

   - Additional keyword arguments passed down to each ``load_tm1_cube_to_csv_file`` call (e.g., ``target_csv_output_dir``, ``delimiter``, ``decimal``, ``clear_source``).

Example Workflow
~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service

    # 1. Define slicing parameters (export each month of 2024)
    params = ["{[Period].[Period].[2024].Children}"]

    # 2. Define the MDX template with a placeholder
    mdx_tmpl = "SELECT {[Version].[Version].[Actual].Children} ON 0 FROM [Sales] WHERE ([Period].[Period].[$Period])"

    # 3. Run the executor to export each month to a separate file
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        asyncio.run(bedrock.async_executor_tm1(
            data_copy_function=bedrock.load_tm1_cube_to_csv_file,
            tm1_service=tm1,
            param_set_mdx_list=params,
            data_mdx_template=mdx_tmpl,
            max_workers=12,

            # Pass-through kwargs for the underlying load_tm1_cube_to_csv_file function
            target_csv_output_dir="./exports/sales_by_month",
            delimiter=";",
            decimal=",",
            async_write=False,  # The executor handles the async part; the worker should be synchronous
            use_blob=True,   # Defaults to False since True needs administrator privilege. Setting true significantly improves performance.
            skip_zeros=True
        ))

async_executor_csv_to_tm1 (for CSV -> TM1)
------------------------------------------

This function orchestrates the parallel loading of multiple CSV files into a TM1 cube.

Parameter Reference
~~~~~~~~~~~~~~~~~~~

1. **target_cube_name** *(required, string)*

   - The name of the destination cube in TM1.

2. **source_directory** *(required, string)*

   - The full path to the directory containing the source CSV files to be loaded.

3. **param_set_mdx_list** *(required, list[string])*

   - A list of MDX set queries that define the target slices. The number of parameter tuples generated should correspond to the number of CSV files.

4. **data_mdx_template** *(optional, string)*

   - An MDX query template used solely for metadata inference by the underlying ``load_csv_data_to_tm1_cube`` function.

5. **target_clear_set_mdx_list** *(required, list[string])*

  - A list of set MDX lists for clearing the target slices of the input cube.
  - *Example*: :python:`["{[Period].[All Months]}", "{[Version].[ForeCast]}"]`

6. ****kwargs** *(optional)*

   - Additional keyword arguments passed down to each ``load_csv_data_to_tm1_cube`` call (e.g., ``delimiter``, ``decimal``, ``use_blob``, ``increment``).

Example Workflow
~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service

    # Assumes a directory "./imports/sales" contains:
    # - US_Sales.csv
    # - EU_Sales.csv
    # - AP_Sales.csv

    # 1. Define parameters to match the files (in the same order)
    params = ["{[Region].[Region].['US', 'EU', 'AP']}"]

    # 2. Define clear templates that use the same parameter
    clear_tmpl = ["{[Version].[Version].[Plan]}", "{[Region].[Region].[$Region]}"]

    # 3. Run the executor
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        asyncio.run(bedrock.async_executor_csv_to_tm1(
            tm1_service=tm1,
            target_cube_name="Sales",
            source_directory="./imports/sales",
            param_set_mdx_list=params,
            clear_param_templates=clear_tmpl,
            max_workers=3,

            # Pass-through kwargs for the underlying load_csv_data_to_tm1_cube function
            delimiter=",",
            use_blob=True   # Defaults to False since True needs administrator privilege. Setting true significantly improves performance.
            async_write=False # The executor handles the async part; the worker should be synchronous
        ))

------

Developer Comments & Performance Tuning
=======================================

.. warning::

    **File and Parameter Pairing is Crucial (for ``async_executor_csv_to_tm1``)**

    The logic of the CSV-to-TM1 executor depends on a direct correspondence between the source files and the TM1 parameters. You must ensure that the **number and order** of CSV files found in the ``source_directory`` (alphabetically) intentionally match the number and order of the parameter tuples generated by ``param_set_mdx_list``. The process will stop at the length of the shorter list.

.. note::

    **Performance Tuning**

    *   **``max_workers``**: The most critical parameter. The optimal value depends on the TM1 server's capacity for concurrent reads (for TM1->CSV) or writes (for CSV->TM1), as well as disk I/O speed.
    *   **Slicing Strategy (for TM1->CSV)**: The effectiveness of the parallelization depends on your slicing strategy in ``param_set_mdx_list``. Good strategies create slices of roughly equal size to ensure all workers finish at around the same time.

.. note::

    **Clearing Behavior (for ``async_executor_csv_to_tm1``)**

    Unlike the SQL executors where clearing is a single global operation, the ``clear_param_templates`` in this function enable **per-worker clearing**. Each worker generates and executes its own clear operation for its specific target slice just before it begins loading its data file. This is ideal for targeted updates.

.. note::

    **Metadata Caching**

    Both executors are optimized to pre-cache relevant cube metadata once before starting the parallel workers, avoiding redundant API calls and improving performance.

------

Conclusion
==========

The asynchronous CSV executors provide a powerful and scalable solution for automating file-based data integration with TM1. By understanding their distinct slicing mechanisms—parameter-based for exports and file-pairing for imports—you can build highly efficient, parallelized ETL processes that significantly reduce the time required for large-scale data loads and archives.