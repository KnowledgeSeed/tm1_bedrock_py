Calculation Input Handler
=========================

``input_handler`` is Bedrock's business-logic pipeline for TM1 writes. It
builds a coordinate domain, applies ordered calculations and mapping steps, and
writes one final value column to a target cube.

Use it when the hard part of the process is not extraction, but deciding how an
entered value should be spread, filtered, allocated, or recalculated across a
TM1 domain.

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

The examples in ``example/main.py`` show common patterns such as:

* equal spreading across a domain;
* proportional spreading from a prior-period or lookup-based ratio;
* conditional assignment with ``if_then`` logic;
* post-calculation cartesian expansion;
* inspection runs with ``do_write=False``.

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

When to choose input_handler
----------------------------

Choose ``input_handler`` when you need a reusable, declarative calculation
pipeline. Choose ``data_copy_intercube`` when you are primarily moving and
remapping existing data rather than computing a new input distribution.
