TM1 Bedrock for Python
======================

``tm1_bedrock_py`` is a high-level integration toolkit for IBM Planning
Analytics (TM1). It builds on TM1py and pandas to move data between TM1, SQL,
and CSV sources, reshape coordinates with mapping pipelines, and manage
dimension structures from tabular input.

The main public interface lives in ``TM1_bedrock_py.bedrock``.

Start with :doc:`usage` for workflow selection and first examples. Use
:doc:`api_reference` when you need the exact current signatures and defaults.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   usage
   data_copy
   dimension_management
   calculations
   tm1_sql
   tm1_csv
   reliability

.. toctree::
   :maxdepth: 2
   :caption: Advanced workflows

   async_executor
   async_sql
   async_csv
   airflow_executor

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api_reference

Project links
-------------

* `Source repository <https://github.com/KnowledgeSeed/tm1_bedrock_py>`_
* `Issue tracker <https://github.com/KnowledgeSeed/tm1_bedrock_py/issues>`_
