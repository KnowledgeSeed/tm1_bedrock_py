Cube, Dimension, and Hierarchy Management
=========================================

For structure management, ``dimension_builder`` is the main entry point. It is
designed for tabular inputs such as Excel extracts, CSV files, SQL result sets,
and already-prepared pandas DataFrames.

Choose the wrapper
------------------

``cube_builder``
   Create cubes from a ``cube -> dimensions`` mapping or copy cube structures
   from another TM1 service.

``dimension_builder``
   Build all hierarchies of a dimension from tabular, SQL, file, or DataFrame
   input.

``hierarchy_builder``
   Build one named hierarchy in an existing dimension.

``dimension_copy`` / ``hierarchy_copy``
   Copy existing structures, optionally between TM1 services and with renames.

``dimension_modify`` / ``hierarchy_modify``
   Export the current normalized schema, pass it to a callable, validate the
   returned DataFrames, and apply the change.

``hierarchy_build_from_attributes``
   Build a hierarchy from one or more element attributes.

``dimension_export``
   Export normalized dimension data to SQL, CSV, XLSX, YAML, or JSON.

Primary workflow: dimension_builder
-----------------------------------

Use ``dimension_builder`` when you want the docs to reflect the actual source
of truth for a dimension: a business-maintained file, a generated DataFrame, or
an upstream SQL extract.

Build a cube
------------

.. code-block:: python

   bedrock.cube_builder(
       tm1_service=tm1,
       build_mode="create_from_map",
       cube_dimension_create_map={
           "Sales": ["Version", "Period", "Product", "Measure"],
       },
       if_cube_exist_strategy="skip",
       missing_dimension_strategy="raise_error",
   )

``if_cube_exist_strategy`` accepts ``"rebuild"``, ``"skip"``, or
``"raise_error"``. In ``copy_from_source`` mode, provide
``copy_source_cubes`` and optionally cube/dimension rename maps.

Build a dimension from parent-child data
----------------------------------------

.. code-block:: python

   bedrock.dimension_builder(
       tm1_service=tm1,
       dimension_name="Product",
       input_format="parent_child",
       build_strategy="safe_rebuild",
       input_datasource="./product.csv",
       parent_column="Parent",
       child_column="Child",
       type_column="ElementType",
       weight_column="Weight",
   )

Input formats:

``parent_child``
   One edge per row. Configure parent, child, weight, type, dimension, and
   hierarchy columns as needed.

``indented_levels``
   Level columns contain values only when they change.

``filled_levels``
   Every row contains its complete hierarchy path.

The project examples also use ``indented_levels`` heavily for Excel-based
dimension maintenance. That format works well when business users want to own
the hierarchy shape in a spreadsheet while Bedrock handles normalization,
validation, and TM1 updates.

Build strategies:

``rebuild``
   Replace the target structure.

``safe_rebuild``
   Rebuild while preserving data safety behavior provided by the builder.

``safe_rebuild_unwind``
   Safe rebuild with unwind behavior for removed relationships.

``update``
   Apply additions and changes while preserving compatible legacy elements.
   Legacy child relationships omitted from the input are reparented under the
   orphan consolidation instead of being left untouched.

Common data sources
-------------------

``input_datasource``
   File path for CSV, XLSX, YAML, or JSON input.

``raw_input_df``
   pandas DataFrame supplied directly by your code.

``sql_engine`` with ``sql_query`` or ``sql_table_name``
   SQL-driven structure generation.

This flexibility is one of the main reasons to use ``dimension_builder`` as
the primary interface instead of building TM1 objects manually.

For advanced pipelines, provide both ``override_input_edges_df`` and
``override_input_elements_df``. Supplying only one is rejected.

Output without building
-----------------------

Set ``output_mode="output"`` to return normalized ``(edges_df, elements_df)``
without changing TM1. ``"build_and_output"`` performs both operations.

Export a dimension
------------------

.. code-block:: python

   bedrock.dimension_export(
       tm1_service=tm1,
       dimension_name="Product",
       output_format="parent_child",
       target_destinations=["csv", "json"],
       file_path_destination="./exports/product",
       include_index=False,
   )

Each requested destination needs its corresponding output parameters. Unknown
or empty destination lists fail before dispatch.

Modify with a callable
----------------------

The callable used by ``dimension_modify`` receives the normalized edge and
element DataFrames and must return a two-item tuple:
``(edges_df, elements_df)``. ``hierarchy_modify`` follows the same contract for
one hierarchy. Returned schemas are validated before changes are applied.

Related wrappers
----------------

Use ``hierarchy_builder`` when you only want to rebuild one hierarchy inside an
existing dimension. Use ``dimension_copy`` and ``hierarchy_copy`` when TM1 is
already the source of truth and you want to clone or migrate structures between
servers.
