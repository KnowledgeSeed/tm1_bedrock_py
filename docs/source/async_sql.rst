Parallel SQL Workflows
======================

TM1 to SQL
----------

``async_executor_tm1_to_sql`` uses TM1 parameter sets to render one MDX query
per worker. Every worker calls ``load_tm1_cube_to_sql_table`` by default.

.. code-block:: python

   asyncio.run(
       bedrock.async_executor_tm1_to_sql(
           tm1_service=tm1,
           sql_engine=engine,
           target_table_name="FactSalesExport",
           param_set_mdx_list=[
               "{[Period].[Period].[2026-01], [Period].[Period].[2026-02]}",
           ],
           data_mdx_template="""
           SELECT {[Measure].[Measure].[Amount]} ON 0
           FROM [Sales]
           WHERE ([Period].[Period].[$Period])
           """,
           clear_target=True,
           sql_delete_statement="DELETE FROM FactSalesExport",
           if_table_exists="append",
           max_workers=4,
           skip_zeros=True,
       )
   )

``clear_target=True`` clears the SQL table once before workers start. Workers
then append their slices. Query-specific TM1 metadata is collected per rendered
MDX so filter dimensions are retained correctly.

SQL to TM1
----------

``async_executor_sql_to_tm1`` counts ``sql_table_for_count``, divides the rows
by ``slice_size``, and formats ``sql_query_template`` with ``{offset}`` and
``{fetch}``.

.. code-block:: python

   sql_template = """
   SELECT Version, Period, Product, Amount AS Value
   FROM dbo.FactSales
   ORDER BY SaleId
   OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY
   """

   asyncio.run(
       bedrock.async_executor_sql_to_tm1(
           tm1_service=tm1,
           sql_engine=engine,
           sql_query_template=sql_template,
           sql_table_for_count="dbo.FactSales",
           target_cube_name="Sales",
           slice_size=100_000,
           max_workers=4,
           target_clear_set_mdx_list=[
               "{[Version].[Version].[Actual]}",
           ],
           use_blob=True,
       )
   )

The template must contain both placeholders. It also needs a deterministic
ordering appropriate to the database. SQL Server commonly uses
``OFFSET ... FETCH``; PostgreSQL and MySQL commonly use
``LIMIT {fetch} OFFSET {offset}``.

Target clearing occurs once before worker fan-out when
``target_clear_set_mdx_list`` is supplied. Per-worker target clearing is
disabled.

Correct pagination
------------------

Use a stable, unique ``ORDER BY`` key. Concurrent changes to the source table
can still cause missed or duplicate rows with offset pagination. For mutable
production sources, load from a stable snapshot or staging table.

Connection capacity
-------------------

Each worker can consume SQL and TM1 resources simultaneously. Keep
``max_workers`` below the SQL pool capacity and validate the production
driver's thread-safety and transaction behavior.
