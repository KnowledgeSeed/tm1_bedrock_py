Parallel CSV Workflows
======================

These wrappers are the parallel versions of the CSV workflows in
:doc:`tm1_csv`.

CSV directory to TM1
--------------------

``async_executor_csv_to_tm1`` sorts all ``*.csv`` files in
``source_directory`` and pairs them with the parameter tuples produced by
``param_set_mdx_list``.

.. code-block:: python

   asyncio.run(
       bedrock.async_executor_csv_to_tm1(
           tm1_service=tm1,
           target_cube_name="Sales",
           source_directory="./imports/by_region",
           param_set_mdx_list=[
               "{[Region].[Region].[APAC], [Region].[Region].[EMEA]}",
           ],
           data_mdx_template="""
           SELECT {[Measure].[Measure].[Amount]} ON 0
           FROM [Sales]
           WHERE ([Region].[Region].[$Region])
           """,
           target_clear_set_mdx_list=[
               "{[Version].[Version].[Actual]}",
           ],
           delimiter=",",
           max_workers=2,
       )
   )

File ordering is deterministic and the file count must exactly match the
number of generated parameter tuples. A missing directory, empty parameter
result, or count mismatch fails before target clearing.

``data_mdx_template`` provides query-specific metadata for each target slice.
The default worker is ``load_csv_data_to_tm1_cube``; a compatible custom
callable can be supplied with ``data_copy_function``.

The target clear runs once before workers start. Each worker receives
``clear_target=False``.

TM1 slices to CSV
-----------------

Use ``async_executor_tm1`` with
``data_copy_function=bedrock.load_tm1_cube_to_csv_file``.

.. code-block:: python

   asyncio.run(
       bedrock.async_executor_tm1(
           tm1_service=tm1,
           param_set_mdx_list=[
               "{[Period].[Period].[2026-01], [Period].[Period].[2026-02]}",
           ],
           data_mdx_template="""
           SELECT {[Measure].[Measure].[Amount]} ON 0
           FROM [Sales]
           WHERE ([Period].[Period].[$Period])
           """,
           data_copy_function=bedrock.load_tm1_cube_to_csv_file,
           target_csv_output_dir="./exports/by_period",
           max_workers=2,
           skip_zeros=True,
       )
   )

Leave ``target_csv_file_name`` unset for parallel exports so each worker
generates a unique timestamped filename.

Operational guidance
--------------------

File I/O, TM1 calls, and transformation work happen concurrently. Confirm the
destination filesystem supports concurrent writes, and avoid source clearing
unless every exported slice is intentionally destructive.
