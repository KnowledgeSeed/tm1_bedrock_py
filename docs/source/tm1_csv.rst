CSV Integration
===============

The CSV wrappers are the lightweight file-based equivalents of the SQL
wrappers. They are useful for scheduled imports and exports, business-managed
files, and intermediate landing zones where a database would be unnecessary.

CSV to TM1
----------

``load_csv_data_to_tm1_cube`` reads a file with pandas-compatible parsing
options, normalizes columns, applies mappings, validates target coordinates,
and writes TM1 data.

.. code-block:: python

   bedrock.load_csv_data_to_tm1_cube(
       tm1_service=tm1,
       target_cube_name="Sales",
       source_csv_file_path="./imports/sales.csv",
       delimiter=";",
       decimal=",",
       csv_column_mapping={
           "VERSION": "Version",
           "PERIOD": "Period",
           "PRODUCT": "Product",
           "AMOUNT": "Value",
       },
       csv_columns_to_drop=["SourceFile"],
       dtype={"PERIOD": "string"},
       clear_target=True,
       target_clear_set_mdx_list=[
           "{[Version].[Version].[Actual]}",
       ],
       async_write=True,
   )

Important parsing parameters:

``delimiter`` / ``decimal``
   File separator and decimal character. When omitted, Bedrock uses locale
   detection with safe defaults.

``dtype``
   pandas read dtype mapping. Use string dtype for identifiers that may contain
   leading zeroes.

``parse_dates``, ``na_values``, ``keep_default_na``
   Standard missing-value and date parsing controls.

``nrows`` / ``chunksize``
   Limit or chunk source reads where appropriate.

``csv_function``
   Custom callable replacing the default CSV extractor.

This wrapper is often the fastest path from a controlled business file into a
TM1 cube.

TM1 to CSV
----------

``load_tm1_cube_to_csv_file`` extracts and transforms TM1 data before writing a
CSV.

.. code-block:: python

   bedrock.load_tm1_cube_to_csv_file(
       tm1_service=tm1,
       data_mdx="""
       SELECT {[Period].[Period].[2026-01]} ON 0
       FROM [Sales]
       WHERE ([Version].[Version].[Actual])
       """,
       target_csv_output_dir="./exports",
       target_csv_file_name="sales_2026_01.csv",
       delimiter=",",
       decimal=".",
       float_format="%.2f",
       na_rep="NULL",
       index=False,
       skip_zeros=True,
   )

Output parameters:

``target_csv_file_name``
   Output filename. Bedrock generates a valid name when omitted.

``target_csv_output_dir``
   Destination directory.

``mode``
   Usually ``"w"`` for overwrite or ``"a"`` for append.

``chunksize``
   Rows per CSV write chunk.

``compression``
   pandas compression string or configuration dictionary.

``csv_function``
   Custom CSV writer callable.

This wrapper is useful for handoffs to other systems, audit snapshots, and
debugging transformed TM1 slices outside the server.

Source clearing
---------------

Set ``clear_source=True`` with ``source_clear_set_mdx_list`` only when the
export is intended to be destructive. Bedrock clears the TM1 source after a
successful, non-empty file export. Empty transformed data skips both the write
and source clear.

Locale and data integrity
-------------------------

Use explicit ``delimiter`` and ``decimal`` values for scheduled integrations.
Do not rely on workstation locale when the same job runs in containers,
Airflow, or servers in another region.
