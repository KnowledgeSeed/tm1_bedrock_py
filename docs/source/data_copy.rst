TM1-to-TM1 Data Workflows
=========================

For current projects, ``data_copy_intercube`` is the primary TM1-to-TM1
workflow. It covers both classic cube-to-cube loads and the more important
real-world case where the target dimensionality or target server differs from
the source.

``data_copy`` uses the same internal pipeline, but is mainly the convenience
wrapper for same-cube writes.

Choose the function
-------------------

Use ``data_copy_intercube`` when:

* writing to another cube;
* renaming or reshaping dimensions;
* adding target-only dimensions;
* writing through another ``TM1Service``;
* or simply standardizing on one TM1-to-TM1 wrapper.

Use ``data_copy`` only when the source MDX cube is also the target cube and
you do not need the extra target-cube argument.

Primary example: intercube redimensionalization
-----------------------------------------------

This is the style of workflow Bedrock is best known for: extract a TM1 slice,
transform coordinates with ordered mapping steps, and write into a different
target cube or server.

.. code-block:: python

   bedrock.data_copy_intercube(
       tm1_service=source_tm1,
       target_tm1_service=target_tm1,
       data_mdx=source_mdx,
       target_cube_name="Reporting",
       source_dim_mapping={"Source Measure": "Amount"},
       related_dimensions={"Source Product": "Product"},
       target_dim_mapping={
           "Scenario": "Loaded",
           "Reporting Measure": "Value",
       },
       async_write=True,
       slice_size_of_dataframe=50_000,
   )

The three redimensionalization arguments are passed through ``**kwargs``:

``source_dim_mapping``
   Keep rows matching each configured source element, then remove those source
   dimension columns.

``related_dimensions``
   Rename source dimension columns to target dimension names.

``target_dim_mapping``
   Add target-only dimensions with one constant element per column.

Source extraction
-----------------

Provide ``data_mdx`` for one query or ``data_mdx_list`` for multiple queries.
``skip_zeros``, ``skip_consolidated_cells``, and
``skip_rule_derived_cells`` are passed to TM1 extraction.

Set ``mdx_function="native_view_extractor"`` to use temporary native views and
subsets. Cleanup is enabled by default and cleanup failures are reported.
A callable ``mdx_function`` may implement a custom extractor.

Mapping steps
-------------

Steps execute in list order and are validated before processing. This ordered
pipeline is the core abstraction behind Bedrock's TM1, SQL, CSV, and
``input_handler`` workflows.

``replace``
   Replace values in one or more columns.

.. code-block:: python

   {
       "method": "replace",
       "mapping": {
           "Version": {"Working": "Budget"},
           "Measure": {"Net Sales": "Revenue"},
       },
   }

``map_and_replace``
   Join a mapping DataFrame on shared columns and replace mapped dimensions.

.. code-block:: python

   {
       "method": "map_and_replace",
       "mapping_df": product_mapping,
       "mapping_dimensions": {"Legacy Product": "Product"},
       "mapping_filter": {"Mapping Type": "Active"},
       "relabel_dimensions": True,
   }

``map_and_join``
   Join selected mapping columns and optionally drop source columns.

.. code-block:: python

   {
       "method": "map_and_join",
       "mapping_df": employee_mapping,
       "joined_columns": ["Department", "Cost Center"],
       "dropped_columns": ["Employee"],
   }

``cartesian``
   Add selected columns from every row of a mapping DataFrame by Cartesian
   product.

``pivot`` / ``unpivot``
   Reshape data using pandas-style index, columns, values, and identifier
   fields.

``basic_reshaping``
   Filter rows, drop columns, add constant columns, and relabel columns in one
   step.

``cartesian_with_set``
   Combine source data with a mapping DataFrame populated from a TM1 set.

Example based on the project examples
-------------------------------------

``example/main.py`` shows a representative multi-step pattern:

* replace one coordinate such as ``Version``;
* map one dimension from a TM1 attribute or mapping cube;
* map another dimension through a second TM1 lookup;
* optionally scale or otherwise transform values;
* clear only the intended target slice before write.

That composition model is what makes ``data_copy_intercube`` the best starting
point for most TM1-to-TM1 integrations.

Same-cube convenience
---------------------

Use ``data_copy`` when the source and target cube are the same.

.. code-block:: python

   bedrock.data_copy(
       tm1_service=tm1,
       data_mdx="""
       SELECT {[Period].[Period].[2026-01]} ON 0
       FROM [Planning]
       WHERE ([Version].[Version].[Working])
       """,
       mapping_steps=[
           {
               "method": "replace",
               "mapping": {"Version": {"Working": "Budget"}},
           }
       ],
       clear_target=True,
       target_clear_set_mdx_list=[
           "{[Period].[Period].[2026-01]}",
           "{[Version].[Version].[Budget]}",
       ],
   )

Mapping data sources
--------------------

A mapping step can receive ``mapping_df`` directly or request mapping data
through ``mapping_mdx``, ``mapping_sql_query``,
``mapping_sql_table_name``, ``mapping_csv_file_path``, or ``set_mdx``.
``shared_mapping`` defines one mapping source that multiple steps can reuse.

Callbacks
---------

``value_function`` transforms values. ``pre_load_function`` receives the final
DataFrame and must return a DataFrame. Supply positional and keyword arguments
with ``pre_load_args`` and ``pre_load_kwargs``.

Missing elements and cell types
-------------------------------

Enable ``check_missing_elements`` to validate coordinates before writing.
``fallback_elements`` can replace missing elements; use
``raise_error_if_missing_found`` to fail instead. For mixed numeric/string
measure cubes, ``cast_cell_type_mapping_on_values=True`` casts values using
target metadata.

Writing and duplicates
----------------------

``async_write`` parallelizes the TM1 write. ``slice_size_of_dataframe`` controls
write batches. ``use_ti`` selects the TI writer and ``use_blob`` enables TM1py's
blob mode where supported and permitted. ``increment=True`` increments existing
cells.

``sum_numeric_duplicates`` handles duplicate numeric coordinates during the
write. ``data_copy_intercube`` also supports
``aggregate_numeric_duplicates`` before loading.

Return value
------------

Normal execution writes data and returns ``None``. Some diagnostic options,
including missing-element output, can return a DataFrame; consult the exact
signature and wrapper docstring in :doc:`api_reference`.
