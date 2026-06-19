Getting Started
===============

TM1 Bedrock is aimed at Python developers who are already comfortable with
TM1py and want reusable integration workflows instead of hand-building the same
extract-transform-load code for every job.

Installation
------------

.. code-block:: bash

   pip install tm1-bedrock-py

Python 3.9 or newer is required. Install ``tm1-bedrock-py[airflow]`` only when
using the optional Airflow task groups.

Create a TM1 connection
-----------------------

Bedrock accepts an existing TM1py ``TM1Service``. Connection settings are not
stored by the library, so your application remains in control of credentials,
session lifecycle, and environment-specific connection setup.

.. code-block:: python

   from TM1py import TM1Service
   from TM1_bedrock_py import bedrock

   with TM1Service(
       address="tm1.example.com",
       port=12354,
       user="svc_bedrock",
       password="...",
       ssl=True,
   ) as tm1:
       bedrock.load_tm1_cube_to_csv_file(
           tm1_service=tm1,
           data_mdx="SELECT {[Period].[Period].[2026-01]} ON 0 FROM [Sales]",
           target_csv_output_dir="./exports",
           target_csv_file_name="sales_2026_01.csv",
           skip_zeros=True,
       )

Choose a workflow
-----------------

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Source
     - Target
     - Function
   * - TM1 cube
     - Another cube or server
     - :py:func:`~TM1_bedrock_py.bedrock.data_copy_intercube`
   * - TM1 dimension data
     - TM1 dimension or hierarchy
     - :py:func:`~TM1_bedrock_py.bedrock.dimension_builder`
   * - Coordinate domain
     - Calculated TM1 input
     - :py:func:`~TM1_bedrock_py.bedrock.input_handler`
   * - SQL
     - TM1 cube
     - :py:func:`~TM1_bedrock_py.bedrock.load_sql_data_to_tm1_cube`
   * - TM1 cube
     - SQL
     - :py:func:`~TM1_bedrock_py.bedrock.load_tm1_cube_to_sql_table`
   * - CSV
     - TM1 cube
     - :py:func:`~TM1_bedrock_py.bedrock.load_csv_data_to_tm1_cube`
   * - TM1 cube
     - CSV
     - :py:func:`~TM1_bedrock_py.bedrock.load_tm1_cube_to_csv_file`
   * - TM1 cube
     - Same cube
     - :py:func:`~TM1_bedrock_py.bedrock.data_copy`

For most TM1-to-TM1 movement, start with
:py:func:`~TM1_bedrock_py.bedrock.data_copy_intercube`. It also covers the
common case where the source and target structures differ. ``data_copy`` is
mainly the convenience wrapper for same-cube writes.

The standard data pipeline
--------------------------

Most data wrappers follow the same sequence:

1. Extract data from TM1, SQL, or CSV.
2. Normalize source column names and coordinate values.
3. Collect source or target cube metadata.
4. Apply ``mapping_steps`` and optional redimensionalization.
5. Apply ``value_function`` and ``pre_load_function`` if supplied.
6. Skip the write when the transformed DataFrame is empty.
7. Clear the target only when requested.
8. Write the result.
9. Clear the source only after a successful non-empty export.

The same shape shows up across TM1, SQL, and CSV wrappers, so once you learn
one workflow the others feel familiar.

Common transformation parameters
--------------------------------

``mapping_steps``
   Ordered transformation dictionaries. See :doc:`data_copy`.

``shared_mapping``
   A mapping source reused by multiple mapping steps.

``source_dim_mapping``
   Filter a source dimension to one element, then drop that column.

``related_dimensions``
   Rename source dimension columns to target dimension names.

``target_dim_mapping``
   Add target-only dimensions with constant element values.

``value_function``
   Callable applied to cell values.

``pre_load_function``
   Callable receiving the final DataFrame before write/export. It must return a
   pandas DataFrame.

``case_and_space_insensitive_inputs``
   Normalize source labels before matching them to cube dimensions.

TM1 redimensionalization shortcuts
----------------------------------

Several wrappers accept these arguments through ``**kwargs``:

``source_dim_mapping``
   Filter a source dimension to one element, then drop that source column.

``related_dimensions``
   Rename source dimension columns to target dimension names.

``target_dim_mapping``
   Add target-only dimensions with constant element values.

These three arguments are especially important for
:py:func:`~TM1_bedrock_py.bedrock.data_copy_intercube`.

Missing-element checks
----------------------

TM1 write wrappers can validate target elements with
``check_missing_elements=True``. Use ``dimensions_to_check`` to limit the
check, ``fallback_elements`` to replace missing coordinates, and
``raise_error_if_missing_found=True`` to stop instead of continuing.

``element_query_mode="bulk"`` collects element information up front.
``"on_demand"`` reduces the initial query but may make more TM1 calls.

Logging
-------

Use ``logging_level`` with ``"DEBUG"``, ``"INFO"``, ``"WARNING"``, or
``"ERROR"``. ``verbose_logging_mode="file"`` writes intermediate DataFrames;
``"print_console"`` prints them. Set ``verbose_logging_output_dir`` for file
output.

Do not put secrets in SQL or MDX text. Bedrock redacts query text in its public
error boundary, but query strings remain trusted executable configuration.

Where to go next
----------------

* :doc:`data_copy` for TM1-to-TM1 mappings and redimensionalization
* :doc:`dimension_management` for dimension and hierarchy builds
* :doc:`calculations` for ``input_handler`` pipelines
* :doc:`tm1_sql` and :doc:`tm1_csv` for external integrations
