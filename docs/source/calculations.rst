Calculation Input Handler
=========================

``input_handler`` builds a coordinate domain, applies a calculation pipeline,
and writes one final numeric column to a target cube.

Build the domain
----------------

Provide one domain source:

``domain_coordinates``
   Mapping of dimension names to one element or a consolidated coordinate.

``domain_mdx``
   MDX query used to derive the coordinate DataFrame.

.. code-block:: python

   result = bedrock.input_handler(
       tm1_service=tm1,
       target_cube_name="Allocation",
       input_value=1,
       domain_coordinates={
           "Version": "Working",
           "Period": "2026:Q1",
       },
       calculation_steps=[
           {
               "name": "Allocation",
               "method": "formula",
               "formula": "Input * 100",
           },
       ],
       input_column_name="Allocation",
       output_final_state_dataframe=True,
       do_write=False,
   )

Pipeline stages
---------------

``pre_calc_mapping_steps``
   Mapping pipeline applied before calculations.

``calculation_steps``
   Ordered calculations. Each step requires ``name`` and ``method``.

``post_calc_mapping_steps``
   Mapping pipeline applied after calculations.

Supported calculation methods include ``constant``, ``sum``, ``sum_group``,
``sumif``, ``count``, ``count_group``, ``countif``, ``count_unique``,
``index``, ``rank_over``, ``index_group``, ``cube_data``, ``query``, ``if``,
``condition``, ``formula``, ``string``, ``template``, ``string_template``, and
``custom``.

The final ``input_column_name`` is renamed to ``Value`` and cast to float.
When omitted, the last calculation step's ``name`` is used, or ``Input`` when
there are no calculation steps.

Writing and dry runs
--------------------

``remove_zero_inputs=True`` removes zero rows before write.
``clear_target`` is honored only when ``do_write=True``.
``use_ti_for_load``, ``use_blob_for_load``, and ``increment`` control the TM1
writer.

Set ``do_write=False`` with ``output_final_state_dataframe=True`` to inspect
the pipeline safely without changing TM1.
