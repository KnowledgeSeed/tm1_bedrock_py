TM1 Bedrock for Python
======================

``tm1_bedrock_py`` provides high-level, composable workflows for IBM Planning
Analytics (TM1). It moves and transforms data between TM1 cubes, SQL databases,
CSV files, and dimension structures while keeping TM1py and pandas available
for custom behavior.

Start with :doc:`usage` to choose a workflow. Use :doc:`api_reference` for the
current public signatures and parameter defaults.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   usage
   reliability
   calculations

.. toctree::
   :maxdepth: 2
   :caption: Data workflows

   data_copy
   tm1_sql
   tm1_csv
   async_executor
   async_sql
   async_csv

.. toctree::
   :maxdepth: 2
   :caption: Structure and orchestration

   dimension_management
   airflow_executor

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api_reference

Project links
-------------

* `Source repository <https://github.com/KnowledgeSeed/tm1_bedrock_py>`_
* `Issue tracker <https://github.com/KnowledgeSeed/tm1_bedrock_py/issues>`_
