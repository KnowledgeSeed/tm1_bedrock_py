.. tm1_bedrock_py documentation master file, created by
   sphinx-quickstart on Mon Aug 11 15:05:37 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to tm1_bedrock_py documentation!
========================================
This project is a Python-based toolkit by Knowledgeseed, designed to streamline data integration and automation tasks with IBM Planning Analytics (TM1).

It leverages the ``TM1py`` library to provide a high-level, configurable, and extensible framework for moving data between TM1 cubes, SQL databases, and CSV files. The toolkit is built with a focus on performance, offering features like asynchronous operations and detailed logging for debugging and optimization.

Check out the :doc:`usage` section for a guide on getting started, and the :doc:`data_copy` and :doc:`tm1_csv` and :doc:`tm1_sql` manuals for a deep dive into the core data manipulation functions.
For high performance use-cases, refer to :doc:`async_executor` and :doc:`async_csv`.

.. toctree::
   :maxdepth: 2
   :caption: Introduction:

   usage

.. toctree::
   :maxdepth: 2
   :caption: Synchronous Executors:

   data_copy
   tm1_csv
   tm1_sql

.. toctree::
   :maxdepth: 2
   :caption: Asynchronous Executors:

   async_executor
   async_csv
   async_tm1_to_sql
   async_sql_to_tm1

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


