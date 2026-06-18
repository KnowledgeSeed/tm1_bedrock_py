Bedrock API Reference
=====================

This page is generated from the installed Python functions, so signatures and
default values match the current package. The task guides explain when to use
each group.

Calculation and cube structure
------------------------------

.. autofunction:: TM1_bedrock_py.bedrock.input_handler

.. autofunction:: TM1_bedrock_py.bedrock.cube_builder

Dimension and hierarchy management
----------------------------------

.. autofunction:: TM1_bedrock_py.bedrock.dimension_builder

.. autofunction:: TM1_bedrock_py.bedrock.hierarchy_builder

.. autofunction:: TM1_bedrock_py.bedrock.dimension_copy

.. autofunction:: TM1_bedrock_py.bedrock.hierarchy_copy

.. autofunction:: TM1_bedrock_py.bedrock.dimension_modify

.. autofunction:: TM1_bedrock_py.bedrock.hierarchy_modify

.. autofunction:: TM1_bedrock_py.bedrock.hierarchy_build_from_attributes

.. autofunction:: TM1_bedrock_py.bedrock.dimension_export

TM1 data movement
-----------------

.. autofunction:: TM1_bedrock_py.bedrock.data_copy

.. autofunction:: TM1_bedrock_py.bedrock.data_copy_intercube

SQL integration
---------------

.. autofunction:: TM1_bedrock_py.bedrock.load_sql_data_to_tm1_cube

.. autofunction:: TM1_bedrock_py.bedrock.load_tm1_cube_to_sql_table

CSV integration
---------------

.. autofunction:: TM1_bedrock_py.bedrock.load_csv_data_to_tm1_cube

.. autofunction:: TM1_bedrock_py.bedrock.load_tm1_cube_to_csv_file

Parallel executors
------------------

.. autofunction:: TM1_bedrock_py.bedrock.async_executor_tm1

.. autofunction:: TM1_bedrock_py.bedrock.async_executor_tm1_to_sql

.. autofunction:: TM1_bedrock_py.bedrock.async_executor_sql_to_tm1

.. autofunction:: TM1_bedrock_py.bedrock.async_executor_csv_to_tm1
