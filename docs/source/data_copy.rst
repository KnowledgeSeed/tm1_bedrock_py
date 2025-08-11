.. _data-copy-manual:

======================================
Data Copy / Data Copy Intercube Manual
======================================

.. contents:: Table of Contents
   :local:
   :depth: 2

.. _introduction:

Introduction
============

The **mission** of ``tm1_bedrock_py``’s **Data Copy** module is to serve as the spiritual successor of TM1 Bedrock’s ``cube.data.copy``. Its primary goal is to **read data from TM1**, transform it using optional **mapping data**, and then **write the results back to TM1**.

At its core, it provides functionality akin to Bedrock’s ``cube.data.copy``—with **upgraded features**. Key design priorities include:

- **Ease of use**
- **Integration flexibility**
- **Auto-parallelization**

In its current form, Data Copy supports:

- **MDX inputs** (e.g., view MDX for data or set MDX for dimension filters)
- **Lists**
- **Dictionaries**
- **Callable function inputs** (for read, write, or clear tasks), alongside **default TM1py-based** operations.

TM1py’s asynchronous capabilities enable **parallel reading and writing** for improved performance.

----

.. _default-operations:

Default Operations
==================

Whether you use **Data Copy** or **Data Copy Intercube**, the **default sequence** of operations is:

1. **Read source data** from TM1 (via TM1py or a custom function).
2. **Retrieve metadata** for both query and cube(s) involved.
3. **Complete the output DataFrame** by appending the relevant dimension and element data (using source metadata).
4. **Read mapping information** for any cube-based mappings.
5. **Perform mapping transformations** (replace, map-and-replace, map-and-join) on the DataFrame.
6. **Redimensionalize** the data as needed (only applicable in *Data Copy Intercube*).
7. **Rearrange** the output DataFrame’s columns/dimensions to match the target cube (using target metadata).
8. **Apply a value function** (optional) for scaling or other calculations.
9. **Clear** the target cube (if so requested).
10. **Write** the (possibly transformed) data back to TM1.

----

.. _data_copy_data_copy_intercube_key_differences:

data_copy, data_copy_intercube key differences
==============================================

- **Data Copy**

  - The target **is the same** as the source cube.
  - ``target_cube_name`` and ``target_metadata`` do **not** apply.
  - **No column joining or redimensionalization** is applicable; dimensionality must remain unchanged.

- **Data Copy Intercube**

  - The target cube **differs** from the source cube.
  - Gathers information about the target into ``target_metadata``.
  - **Column joining and redimensionalization** (column addition or removal) are permitted because the two cubes’ dimensionalities may differ.

----

.. _parameter-reference:

Parameter Reference
===================

Below is a complete, itemized listing of all possible function inputs. Most parameters are **optional** and have defaults, but some are required based on context (e.g., certain parameters are mandatory when reading data, some only apply to Intercube operations, etc.).

.. _tm1-service-and-mdx-execution:

TM1 Service and MDX Execution
-----------------------------

1. **tm1_service** *(optional)*

   - A ``TM1Service`` object used by the default read/write/clear functionality.
   - **Developer comment**: If you rely on the built-in MDX executor and metadata collector, a valid ``tm1_service`` must be supplied.

2. **data_mdx** *(optional, string)*

   - An MDX query for the **source data** (valid view MDX format).

3. **data_mdx_list** *(optional, list[string])*

   - A list of MDX queries for the source data.
   - When multiple MDX queries are provided, **asynchronous reading** is enabled.
   - **Developer comment**: Either ``data_mdx`` or ``data_mdx_list`` **must** be specified (unless using purely custom callables).

4. **mdx_function** *(optional, callable)*

   - A **custom function** used to execute the MDX queries for data retrieval.
   - Defaults to ``tm1py``’s ``execute_mdx_dataframe`` (or ``execute_mdx_dataframe_async`` if multiple queries).

----

.. _metadata-functions:

Metadata Functions
------------------

1. **data_metadata_function** *(optional, callable)*

   - A **custom metadata function** (or a “value as function”) for retrieving source metadata.
   - If omitted, metadata is determined automatically.

2. **target_metadata_function** *(optional, callable; only in *Data Copy Intercube*)*

   - A **custom metadata function** (or value) for retrieving the target’s metadata.
   - If omitted, metadata is determined automatically.
   - **Developer comment**: In ``data_copy_intercube``, you must supply either ``target_cube_name`` or ``target_metadata_function`` so the system knows which target cube to work with.

----

.. _skipping-values-and-cells:

Skipping Values and Cells
-------------------------

1. **skip_zeros** *(optional, boolean; default = False)*

   - Skip zero values in source data.

2. **skip_consolidated_cells** *(optional, boolean; default = False)*

   - Skip consolidated cells in source data.

3. **skip_rule_derived_cells** *(optional, boolean; default = False)*

   - Skip rule-derived cells in source data.

----

.. _target-cube-definition-intercube:

Target Cube Definition (Intercube)
----------------------------------

- **target_cube_name** *(optional, string; only in *Data Copy Intercube*)*

- Name of the target cube to which data is written.
- Retrieved from target metadata if omitted.
- **Developer comment**: Either ``target_cube_name`` or ``target_metadata_function`` must be specified.

----

.. _shared_mapping_data:

Shared Mapping data
-------------------

**Example structure:**

.. code-block:: python

    shared_mapping = {
        "mapping_df":shared_mapping_df_name
        "mapping_mdx":"SELECT ... ON COLUMNS, ... ON ROWS ... FROM ... WHERE"
        "mapping_metadata_function":shared_mapping_metadata_function
    }

**Step-Specific Mapping Arguments**:

- ``mapping_mdx``: MDX that returns a mapping DataFrame.
- ``mapping_df``: Pass a DataFrame directly if you already have it.
- ``mapping_metadata_function``: Custom metadata function for the mapping. Pass if you already have it, makes DataFrame retrieval faster.

----

.. _mapping-steps:

Mapping Steps
-------------

**mapping_steps** *(optional, list[dict])*

- **Core transformation input** for Data Copy and Data Copy Intercube.
- A **list of nested dictionaries**, each describing one mapping step.
- Steps run **in the order** they’re listed.

**Example structure**:

.. code-block:: python

    mapping_steps = [
      {
        "method": "replace",
        "mapping": {
          "dim1tochange": {"source": "target"},
          "dim2tochange": {"source3": "target3", "source4": "target4"}
        }
      },
      {
        "method": "map_and_replace",
        "mapping_mdx": "SELECT ...",
        "mapping_metadata_function": some_metadata_function,
        "mapping_df": some_mapping_dataframe,
        "mapping_filter": {
          "dim": "element",
          "dim2": "element2"
        },
        "mapping_dimensions": {
          "dimname_to_change_in_source": "dimname_in_mapping"
        },
        "relabel_dimensions": false
      },
      {
        "method": "map_and_join",
        "mapping_mdx": "SELECT ...",
        "mapping_metadata_function": another_metadata_function,
        "mapping_df": another_mapping_dataframe,
        "mapping_filter": {
          "dim": "element",
          "dim2": "element2"
        },
        "joined_columns": ["col1", "col2"],
        "dropped_columns": ["col3", "col4"]
      }
    ]

.. _method-replace:

Method: **replace**
~~~~~~~~~~~~~~~~~~~

- **Most basic** transformation step.
- Replaces specified **source values** (in specified dimensions) with corresponding target values.
- Supports multiple **dimensions** and multiple **source→target** pairs per dimension.
- Required key: ``"method": "replace"``.

**Mapping example**:

.. code-block:: json

    "mapping": {
      "dim_to_map": { "source1": "target1", "source2": "target2" },
      "dim2_to_map": { "source3": "target3" }
    }

**Example transformation**:

**Input DataFrame**

.. code-block:: text

    period   dim_to_map   dim2_to_map   measure   value
    202202   source1      source3       value     100
    202203   source2      other         value     100
    202202   other        source3       value     100
    202203   other        other         value     100

**Output DataFrame**

.. code-block:: text

    period   dim_to_map   dim2_to_map   measure   value
    202202   target1      target3       value     100
    202203   target2      other         value     100
    202202   other        target3       value     100
    202203   other        other         value     100

----

.. _method-map_and_replace:

Method: **map_and_replace**
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Joins** the source and mapping DataFrames on all **shared columns** (except those in ``"mapping_dimensions"``).
- **Replaces** source dimension values with **mapped** dimension values.
- Can optionally **relabel the column** name in the source DataFrame if ``"relabel_dimensions": true``.
- Required key: ``"method": "map_and_replace"``.
- You must define **either** step-specific or shared mapping data for it to work.

**Step-Specific Mapping Arguments**:

- ``mapping_mdx``: MDX that returns a mapping DataFrame.
- ``mapping_df``: Pass a DataFrame directly if you already have it.
- ``mapping_metadata_function``: Custom metadata function for the mapping.
- ``mapping_filter`` *(optional)*: Filters the mapping DataFrame locally. E.g., ``{"dimension": "element"}``.
- ``mapping_dimensions``: Dict specifying which dimension(s) to align between the source and mapping DataFrames.
- ``relabel_dimensions`` *(optional)*: Only applies to Data Copy Intercube. If ``true``, rename the dimension itself in the DataFrame after replacement.

**Example #1**

.. code-block:: python

    {
      "method": "map_and_replace",
      "mapping_df": employee_settings_df,
      "mapping_filter": {
        "Employee Settings": "ORG_UNIT_PARENT"
      },
      "mapping_dimensions": {
        "OrgUnit": "Value"
      },
      "relabel_dimensions": false
    }

**Source DataFrame**

.. code-block:: text

    Period   Employee    OrgUnit   Measure   Value
    202202   00000001    ABC123A   Value     120
    202202   00000002    ABC123A   Value     120
    202202   00000003    XYZ123X   Value     120
    202202   00000004    XYZ123X   Value     120

**Mapping DataFrame** (``employee_settings_df``)

.. code-block:: text

    Period   Employee    Employee Settings   Value
    202202   00000001    ORG_UNIT_PARENT     AAA0001
    202202   00000002    ORG_UNIT_PARENT     AAA0002
    202202   00000003    ORG_UNIT_PARENT     AAA0001
    202202   00000004    ORG_UNIT_PARENT     AAA0002
    202202   00000001    COST_POS_TYPE       AM
    202202   00000002    COST_POS_TYPE       KAM
    202202   00000003    COST_POS_TYPE       Team Leader
    202202   00000004    COST_POS_TYPE       Manager

**Output DataFrame**

.. code-block:: text

    Period   Employee    OrgUnit   Measure   Value
    202202   00000001    AAA0001   Value     120
    202202   00000002    AAA0002   Value     120
    202202   00000003    AAA0001   Value     120
    202202   00000004    AAA0002   Value     120

**Example #2**

.. code-block:: python

    {
      "method": "map_and_replace",
      "mapping_df": employee_to_orgunit,
      "mapping_dimensions": {
        "OrgUnit": "OrgUnit2"
      },
      "relabel_dimensions": true
    }

**Source DataFrame**

.. code-block:: text

    Period   Employee    OrgUnit   Measure   Value
    202202   00000001    ABC123A   Value     120
    202202   00000002    ABC123A   Value     120
    202202   00000003    XYZ123X   Value     120
    202202   00000004    XYZ123X   Value     120

**Mapping DataFrame** (``employee_to_orgunit``)

.. code-block:: text

    Period   Employee    OrgUnit2   Value
    202202   00000001    AAA0001    1
    202202   00000002    AAA0002    1
    202202   00000003    AAA0001    1
    202202   00000004    AAA0002    1

**Output DataFrame**

.. code-block:: text

    Period   Employee    OrgUnit2   Measure   Value
    202202   00000001    AAA0001    Value     120
    202202   00000002    AAA0002    Value     120
    202202   00000003    AAA0001    Value     120
    202202   00000004    AAA0002    Value     120

----

.. _method-map_and_join:

Method: **map_and_join**
~~~~~~~~~~~~~~~~~~~~~~~~

- **Joins** additional columns (``"joined_columns"``) from the mapping DataFrame to the source DataFrame based on shared dimensions.
- **Drops** columns listed under ``"dropped_columns"``.
- Required key: ``"method": "map_and_join"``.
- Must rely on either step-specific or shared mapping data.

**Step-Specific Mapping Arguments**:

- ``mapping_mdx``: MDX for retrieving the mapping DataFrame (if needed).
- ``mapping_df``: Use an existing DataFrame if you have one.
- ``mapping_metadata_function``: Metadata function for the mapping.
- ``mapping_filter``: Dict for filtering the mapping DataFrame.
- ``joined_columns`` *(required)*: Columns to join from the mapping into the source.
- ``dropped_columns`` *(optional)*: Columns to remove after joining.

**Example**

.. code-block:: python

    {
      "method": "map_and_join",
      "mapping_df": employee_settings_df,
      "joined_columns": ["Sales Channel", "Specialism"],
      "dropped_columns": ["Employee"]
    }

**Source DataFrame**

.. code-block:: text

    Period   Employee    Orgunit   Measure   Value
    202201   00000001    abc123a   cost001   10
    202201   00000002    abc123a   cost001   10
    202202   00000001    def345a   cost001   10
    202202   00000002    def345a   cost001   10

**Mapping DataFrame**

.. code-block:: text

    Period   Employee    Sales Channel   Specialism    Cost Position Type
    202201   00000001    temp            IT            KAM
    202201   00000002    perm            finance       AM
    202202   00000001    perm            engineering   Team Leader
    202202   00000002    perm            engineering   Manager

**Output DataFrame**

.. code-block:: text

    Period   Orgunit   Measure   Value   Sales Channel   Specialism
    202201   abc123a   cost001   10      temp            IT
    202201   abc123a   cost001   10      perm            finance
    202202   def345a   cost001   10      perm            engineering
    202202   def345a   cost001   10      perm            engineering

**Developer comments**:

- The column order will be further **rearranged** to match the **target cube**.
- **Numeric duplicates** (same dimensionality) can be **summed** if ``"sum_numeric_duplicates": true``.

----

.. _additional-intercube-parameters:

Additional Intercube Parameters
-------------------------------

The following parameters only apply to **Data Copy Intercube**, where source and target cubes differ in dimensionality or naming.

.. _source_dim_mapping:

1. **source_dim_mapping** *(optional, dict)*

   - Declares dimensions **present in the source** but **not present** in the target.
   - For each such dimension, specify an element to **filter**. Rows matching that element remain; all others are excluded. Then the dimension (column) is **dropped**.

   **Example**

   .. code-block:: text

       # Source DataFrame
       Period   Employee   Orgunit   Measure   Value
       202201   00000001   Total     cost001   1000
       202201   00000001   Total     cost002   2000
       202201   00000001   abc123a   cost001   10
       202201   00000001   def345a   cost002   20

       # source_dim_mapping
       {
         "Orgunit": "Total",
         "Measure": "cost001"
       }

       # Output DataFrame
       Period   Employee   Value
       202201   00000001   1000

.. _related_dimensions:

2. **related_dimensions** *(optional, dict)*

   - Defines relationships between **source dimension names** and **target dimension names**, preserving their **elements**.
   - Essentially **renames** the column in the DataFrame from source dimension to target dimension.

   **Example**

   .. code-block:: text

       # Source DataFrame
       Period   Employee   Orgunit   Measure   Value
       202201   00000001   Total     cost001   1000
       202201   00000001   Total     cost002   2000
       202201   00000001   abc123a   cost001   10
       202201   00000001   def345a   cost002   20

       # related_dimensions
       {
         "Employee": "Key Account Manager"
       }

       # Output DataFrame
       Period   Key Account Manager   Orgunit   Measure   Value
       202201   00000001             Total     cost001   1000
       202201   00000001             Total     cost002   2000
       202201   00000001             abc123a   cost001   10
       202201   00000001             def345a   cost002   20

.. _target_dim_mapping:

3. **target_dim_mapping** *(optional, dict)*

   - Declares dimensions **present in the target** but **not** in the (post-mapping) DataFrame.
   - For each missing dimension, a new column is **added** to the DataFrame, assigning a **single element** to all rows.

   **Example**

   .. code-block:: text

       # Source DataFrame
       Period   Employee   Orgunit   Value
       202201   00000001   Total     1000
       202201   00000001   Total     2000
       202201   00000001   abc123a   10
       202201   00000001   def345a   20

       # target_dim_mapping
       {
         "Lineitem": "Salary Costs",
         "Measure": "Value"
       }

       # Output DataFrame
       Period   Employee   Orgunit   Value   Lineitem       Measure
       202201   00000001   Total     1000    Salary Costs   Value
       202201   00000001   Total     2000    Salary Costs   Value
       202201   00000001   abc123a   10      Salary Costs   Value
       202201   00000001   def345a   20      Salary Costs   Value

----

.. _value-manipulation:

Value Manipulation
------------------

1. **value_function** *(optional, callable)*

- A function that **transforms** each numeric value in the DataFrame.

- Example:

  .. code-block:: python

      def multiply_by_two(x):
          return x * 2

- **Input DataFrame**

  .. code-block:: text

      Period   Employee   Orgunit   Value
      202201   00000001   Total     1000
      202201   00000001   Total     2000

- **Result**

  .. code-block:: text

      Period   Employee   Orgunit   Value
      202201   00000001   Total     2000
      202201   00000001   Total     4000

----

.. _clearing-options:

Clearing Options
----------------

1. **clear_target** *(optional, boolean; default = False)*
   - If ``True``, **clears** the target cube or slice before writing.

2. **clear_set_mdx_list** *(optional, list[string])*

   - A list of **set MDX** expressions for clearing.

   - **Example**

     .. code-block:: python

         clear_set_mdx_list = [
           "TM1FilterBylevel({TM1DrillDownMember({[Periods].[Fiscal Year].[2024]}, ALL, RECURSIVE)}, 0)",
           "{[Versions].[Versions].[Plan]}"
         ]

----

.. _performance-writing-modes:

Performance & Writing Modes
---------------------------

1. **async_write** *(optional, boolean; default = False)*
   - Use TM1py’s **asynchronous** write mode.

2. **use_ti** *(optional, boolean; default = False)*

   - Use an **unbound TurboIntegrator process** to perform the write.
   - Requires **admin privileges** and often yields higher performance.

3. **use_blob** *(optional, boolean; default = False)*

   - Use a **blob** for writing, also requiring **admin privileges**.
   - Typically **10× faster** than TI-based writes alone.

4. **increment** *(optional, boolean; default = False)*
   - If ``True``, **increments** the target cube values instead of overwriting them.

5. **sum_numeric_duplicates** *(optional, boolean; default = True)*
   - Whether to **sum** numeric values for rows that share **identical dimensionality**

----

.. _additional-keyword-arguments:

Additional Keyword Arguments
----------------------------

****kwargs** *(optional)*

- Extra keyword arguments passed to **custom callables** if needed.

----

.. _example_workflow:

Example Workflow
================

**Basic Data Copy Usage:**
--------------------------

.. code-block:: python

    from TM1py import TM1Service
    from tm1_bedrock_py import data_copy

    with TM1Service(address='localhost', user='admin', password='apple', ssl=True) as tm1:
        data_copy(
            tm1_service=tm1,
            data_mdx="SELECT ...",
            skip_zeros=True,
            target_cube_name="Target_Cube",
            mapping_steps=[{"method":"replace", "mapping":{"dimension":{"sourceelem":"targetelem"}}}],
            async_write=True
        )

**Function definitions with complete parameter lists and type declarations**
-----------------------------------------------------------------------------

.. code-block:: python

    def data_copy_intercube(
            tm1_service: Optional[Any],
            data_mdx: Optional[str] = None,
            mdx_function: Optional[Callable[..., DataFrame]] = None,
            data_mdx_list: Optional[list[str]] = None,
            skip_zeros: Optional[bool] = False,
            skip_consolidated_cells: Optional[bool] = False,
            skip_rule_derived_cells: Optional[bool] = False,
            target_cube_name: Optional[str] = None,
            target_metadata_function: Optional[Callable[..., DataFrame]] = None,
            data_metadata_function: Optional[Callable[..., DataFrame]] = None,
            mapping_steps: Optional[List[Dict]] = None,
            shared_mapping_df: Optional[DataFrame] = None,
            shared_mapping_mdx: Optional[str] = None,
            shared_mapping_metadata_function: Optional[Callable[..., Any]] = None,
            source_dim_mapping: Optional[dict] = None,
            related_dimensions: Optional[dict] = None,
            target_dim_mapping: Optional[dict] = None,
            value_function: Optional[Callable[..., Any]] = None,
            clear_set_mdx_list: Optional[List[str]] = None,
            clear_target: Optional[bool] = False,
            async_write: bool = False,
            use_ti: bool = False,
            use_blob: bool = False,
            increment: bool = False,
            sum_numeric_duplicates: bool = True,
            **kwargs
    ) -> None:
        pass

    def data_copy(
            tm1_service: Optional[Any],
            data_mdx: Optional[str] = None,
            mdx_function: Optional[Callable[..., DataFrame]] = None,
            data_mdx_list: Optional[list[str]] = None,
            skip_zeros: Optional[bool] = False,
            skip_consolidated_cells: Optional[bool] = False,
            skip_rule_derived_cells: Optional[bool] = False,
            data_metadata_function: Optional[Callable[..., DataFrame]] = None,
            mapping_steps: Optional[List[Dict]] = None,
            shared_mapping_df: Optional[DataFrame] = None,
            shared_mapping_mdx: Optional[str] = None,
            shared_mapping_metadata_function: Optional[Callable[..., Any]] = None,
            value_function: Optional[Callable[..., Any]] = None,
            clear_set_mdx_list: Optional[List[str]] = None,
            clear_target: Optional[bool] = False,
            async_write: bool = False,
            use_ti: bool = False,
            use_blob: bool = False,
            increment: bool = False,
            sum_numeric_duplicates: bool = True,
            **kwargs
    ) -> None:
        pass

.. _developer-comments:

Developer Comments
==================

- **If the default MDX executor and metadata collector** are used, you must pass a valid ``tm1_service``.
- **When applying** the asynchronous or TI-based writes, **administrator rights** may be required.
- **Column order** is eventually rearranged by ``dataframe_rearrange_dimensions`` to align with the target cube’s shape.
- **Numeric duplicates** can be automatically summed by enabling ``sum_numeric_duplicates``.
- **Either** ``data_mdx`` **or** ``data_mdx_list`` must be present (unless you supply purely custom read logic).
- For **Data Copy Intercube**, you must define either **``target_cube_name``** or **``target_metadata_function``**.

----

.. _conclusion:

Conclusion
==========

This manual describes **every aspect** of **tm1_bedrock_py**’s **Data Copy** and **Data Copy Intercube** modules. It details:

1. How to **read** source data (using MDX queries or custom functions)
2. **Transform** data through mapping steps, dimension manipulation, or value scaling
3. **Write** the resultant data to the same or a different cube (with optional clearance, asynchronous writing, TurboIntegrator, or blob usage)

By combining these building blocks, you can adapt your data-copy processes to a wide range of TM1 tasks while leveraging additional features like auto-parallelization, dimension re-labeling, and custom transformations.