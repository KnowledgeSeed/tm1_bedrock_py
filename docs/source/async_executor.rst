Parallel Execution
==================

Bedrock's async executors use a Python ``ThreadPoolExecutor`` behind an
``async`` public function. Call them with ``asyncio.run`` from synchronous code
or ``await`` them inside an existing event loop.

General rules
-------------

* ``max_workers`` must be a positive integer.
* Each worker receives ``async_write=False`` because concurrency is already
  managed by the executor.
* Mapping DataFrames and reusable metadata are prepared before fan-out where
  possible.
* Workers are allowed to finish; the parent then raises if any worker failed.
* Rendered SQL and MDX are redacted from failure logs.
* Choose independent, non-overlapping slices unless writes are intentionally
  incremental.

TM1 parameter slicing
---------------------

``async_executor_tm1`` evaluates each set MDX in ``param_set_mdx_list``. The
dimension names become ``string.Template`` placeholders in
``data_mdx_template``. The Cartesian product of returned elements defines the
worker calls.

.. code-block:: python

   import asyncio

   asyncio.run(
       bedrock.async_executor_tm1(
           tm1_service=tm1,
           param_set_mdx_list=[
               "{[Period].[Period].[2026-01], [Period].[Period].[2026-02]}",
               "{[Version].[Version].[Actual], [Version].[Version].[Budget]}",
           ],
           data_mdx_template="""
           SELECT {[Measure].[Measure].[Amount]} ON 0
           FROM [Sales]
           WHERE (
             [Period].[Period].[$Period],
             [Version].[Version].[$Version]
           )
           """,
           data_copy_function=bedrock.data_copy_intercube,
           target_cube_name="Sales Reporting",
           max_workers=4,
           skip_zeros=True,
       )
   )

The template must contain every placeholder inferred from the parameter sets.
An empty parameter result is rejected before target clearing or worker launch.

``data_copy_function`` defaults to ``data_copy``. It may be
``data_copy_intercube``, ``load_tm1_cube_to_sql_table``,
``load_tm1_cube_to_csv_file``, or a compatible custom callable. Function-
specific values are passed through ``**kwargs``.

Target clearing
---------------

When ``target_clear_set_mdx_list`` is supplied, ``async_executor_tm1`` clears
the target once before fan-out and disables per-worker target clears. For
intercube work the target service is used.

Do not pass a broad clear unless all workers together replace that complete
space.

Performance
-----------

Start with a small worker count and measure source query time, target write
time, TM1 CPU, SQL connection limits, and memory. More workers can make a
shared TM1 server slower. Prefer balanced slices over simply increasing
``max_workers``.

See :doc:`async_sql` and :doc:`async_csv` for specialized executors.
