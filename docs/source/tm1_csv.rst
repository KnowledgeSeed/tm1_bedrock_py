.. role:: python(code)
   :language: python

.. role:: json(code)
   :language: json

.. role:: yaml(code)
   :language: yaml

=================================
TM1 / CSV Data Integration Manual
=================================

.. contents:: Table of Contents
   :depth: 2

------

.. _introduction:

Introduction
============

The **mission** of :python:`tm1_bedrock_py`’s **TM1/CSV Integration** functions is to provide a robust and seamless bridge between TM1 cubes and CSV flat files.

This functions are designed with key priorities in mind:

- **Robustness and Data Integrity**: Automatically handles common data pitfalls, such as mismatched decimal separators and inconsistent data types.
- **Flexibility**: Provides extensive control over both CSV parsing and TM1 writing.
- **Consistency**: Leverages the same mapping and transformation engine used by the :doc:`data_copy / data_copy_intercube <data_copy>` functions.

The module provides two core functions:

- :python:`load_csv_data_to_tm1_cube`: For loading and transforming data **from a CSV file into a TM1 cube**.
- :python:`load_tm1_cube_to_csv_file`: For extracting and transforming data **from a TM1 cube into a CSV file**.

------

.. _default_operations:

Default Operations
==================

The two functions follow a logical, sequential workflow to ensure data is processed correctly.

**load_csv_data_to_tm1_cube (CSV -> TM1)**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.  **Read and Parse CSV**: The source CSV file is read into a pandas DataFrame using highly configurable parsing options.
2.  **Normalize DataFrame**: The raw tabular data is normalized into a TM1-ready format (one value per row with dimension columns) by renaming columns and identifying the `Value` column.
3.  **Apply Mapping Transformations**: The mapping engine\ :doc:`[1] <data_copy>` (`replace`, `map_and_replace`, `map_and_join`) is used to transform the data.
4.  **Redimensionalize**: The DataFrame is transformed as needed to match the structure of the target TM1 cube.
5.  **Validate Data Types**: The function automatically ensures all dimension columns are strings and the `Value` column's type matches the target TM1 measure's type (Numeric or String).
6.  **Clear Target Cube**: If requested, the target slice in the TM1 cube is cleared.
7.  **Write to TM1**: The resulting DataFrame is written to the TM1 cube.

**load_tm1_cube_to_csv_file (TM1 -> CSV)**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.  **Read Source Data**: Data is extracted from the source TM1 cube using an MDX query.
2.  **Apply Mapping Transformations**: The same mapping engine\ :doc:`[1] <data_copy>` is used to transform the extracted data.
3.  **Redimensionalize**: Columns are added, removed, or renamed as needed.
4.  **Clean for Export**: A robust data cleaning step ensures that numeric values are correctly formatted as numbers (not locale-specific strings) to respect the `decimal` parameter during the write phase.
5.  **Write to CSV**: The resulting DataFrame is written to the target CSV file using configurable formatting options.
6.  **Clear Source Cube**: If requested, the source slice in the TM1 cube is cleared.

------

.. _parameter_reference:

Parameter Reference
===================

Below is a complete reference for all function parameters, grouped by category.

.. _tm1_connection_data:

TM1 Connection & Data
~~~~~~~~~~~~~~~~~~~~~

1. **tm1_service** *(required)*

   - A valid `TM1Service` object for connecting to the TM1 instance.

2. **target_cube_name** *(string; only in `load_csv_data_to_tm1_cube`)*

   - The name of the target TM1 cube where data will be written.

3. **data_mdx** *(string; only in `load_tm1_cube_to_csv_file`)*

   - An MDX query to extract the source data from a TM1 cube.

.. _csv_source_config:

CSV Source Configuration (`load_csv_data_to_tm1_cube`)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **source_csv_file_path** *(required, string)*

   - The full path to the source CSV file.

2. **delimiter** *(optional, string)*

   - The character used to separate fields in the CSV file (e.g., `','`, `';'`). Passed to `pandas.read_csv`.
   - Defaults to local delimiter value.

3. **decimal** *(optional, string)*

   - The character to recognize as a decimal point (e.g., `'.'`, `','`). This is crucial for correctly parsing numbers from files created in different locales. Passed to `pandas.read_csv`.
   - Defaults to locale-specific decimal separator value.

4. **csv_column_mapping** *(optional, dict)*

   - A dictionary to rename columns from the CSV source to match TM1 dimension names. Example: :json:`{"PRODUCT_CODE": "Product"}`.

5. **csv_value_column_name** *(optional, string)*

   - The name of the column in the CSV that contains the data values. This column will be automatically renamed to `Value`.

6. **csv_columns_to_keep / drop_other_sql_columns** *(optional)*

   - A list of columns to keep and a boolean to drop all others, allowing you to filter the source data.

7. **validate_datatypes** *(optional, boolean; default=True)*

   - If `True`, automatically validates and casts the `Value` column to match the target TM1 measure's type (Numeric or String).

8. ****kwargs** *(optional)*

   - Additional pandas.read_csv parameters
   - The function also accepts and passes through many other `pandas.read_csv` arguments, including `dtype`, `nrows`, `chunksize`, `parse_dates`, `na_values`, `keep_default_na`, `low_memory`, and `memory_map` for fine-grained control over parsing.

.. _csv_target_config:

CSV Target Configuration (`load_tm1_cube_to_csv_file`)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **target_csv_file_name** *(optional, string)*

   - The name for the output CSV file. If omitted, a name is generated from the name of the source cube and a timestamp.

2. **target_csv_output_dir** *(optional, string)*

   - The directory where the output CSV file will be saved. Defaults to `./dataframe_to_csv`.

3. **mode** *(optional, string; default='w')*

   - The file write mode. `'w'` to overwrite the file, `'a'` to append.

4. **delimiter / decimal** *(optional, string)*

   - The delimiter and decimal characters to use in the output CSV file. The function includes robust pre-processing to ensure the `decimal` parameter is always respected.
   - Defaults to locale-specific delimiter / decimal separator values.

5. **float_format** *(optional, string)*

   - A format string for floating-point numbers, e.g., `'%.2f'`.

6. **na_rep** *(optional, string; default='NULL')*

   - The string representation to use for missing (`NaN`) values.

7. **index** *(optional, boolean; default=False)*

   - If `True`, writes the DataFrame index as a column in the CSV.

.. _shared_logic_params:

Shared Logic (Mapping, Transformation, Clearing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CSV integration functions leverage the same transformation engine as the `data_copy` functions. The following parameters work identically. For detailed examples of the mapping methods, please refer to the :doc:`Data Copy <data_copy>` manual.

- **shared_mapping** and **mapping_steps**: For applying `replace`, `map_and_replace`, and `map_and_join` transformations.
- **source_dim_mapping**, **related_dimensions**, **target_dim_mapping**: For redimensionalizing the DataFrame to match the target structure.
- **value_function**: For applying a custom function to the `Value` column.
- **clear_target** / **clear_source**: Booleans to enable clearing of the target (in TM1) or source (in TM1).
- **target_clear_set_mdx_list** / **source_clear_set_mdx_list**: MDX sets to define the slice to be cleared.
- **Performance & Writing Modes**: Parameters like `async_write` and `use_blob` apply to the TM1 write portion of the `load_csv_data_to_tm1_cube` function.

------

.. _example_workflow:

Example Workflow
================

**Example 1: Loading a CSV file into TM1**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service

    # Define how to map CSV columns to TM1 dimension names
    column_map = {
        'REGION_NAME': 'Region',
        'PRODUCT_SKU': 'Product',
        'SALES_TOTAL': 'Value'
    }

    # Load a semicolon-delimited CSV with comma decimals
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        bedrock.load_csv_data_to_tm1_cube(
            tm1_service=tm1,
            target_cube_name="Sales",
            source_csv_file_path="C:\\data\\europe_sales.csv",
            delimiter=";",
            decimal=",",
            csv_column_mapping=column_map,
            clear_target=True,
            target_clear_set_mdx_list=["{[Version].[Version].[Actual]}"],
            async_write=True
        )

**Example 2: Exporting TM1 data to a CSV file**
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from TM1_bedrock_py import bedrock
    from TM1py import TM1Service

    # Export a slice of a TM1 cube to a CSV file
    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        bedrock.load_tm1_cube_to_csv_file(
            tm1_service=tm1,
            data_mdx="SELECT {[Version].[Version].[Budget]} ON 0 FROM [Planning]",
            target_csv_output_dir="C:\\archive\\planning",
            target_csv_file_name="budget_export.csv",
            delimiter=",",
            decimal=".",
            skip_zeros=True
        )

------

.. _developer_comments:

Developer Comments
==================

.. note::
   **Data Integrity is Key**: The `decimal` parameter is critical for both reading and writing CSVs, especially when working with data from different regions. This module's built-in data cleaning ensures that numeric data is handled correctly, preventing common data corruption issues.

.. note::
   **Automatic Validation**: The `validate_datatypes=True` parameter in `load_csv_data_to_tm1_cube` is a powerful safety feature. It automatically prepares your DataFrame to meet the strict data type requirements of the TM1 REST API, preventing load failures.

.. note::
   **Admin Rights**: High-performance TM1 writing modes like `async_write=True` and `use_blob=True` may require administrator privileges on the TM1 server.

------

.. _conclusion:

Conclusion
==========

This manual describes the core functionality of the **TM1/CSV Integration** module. It details how to reliably:

1.  **Read** data from either TM1 or a CSV file.
2.  **Transform** the data using the toolkit's consistent and powerful mapping engine.
3.  **Write** the final, clean data to the other system with robust formatting and data integrity checks.

By providing a flexible and resilient bridge between TM1 and the universal format of CSV, this module empowers developers to build sophisticated and reliable data loading and archiving workflows.