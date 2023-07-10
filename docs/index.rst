.. algoseek-connector documentation master file, created by
   sphinx-quickstart on Thu Jun 29 16:56:51 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to algoseek-connector's documentation!
==============================================

Algoseek-connector is a Python library that enables data retrieval from
datasets by creating sql-like queries using method chaining:

.. code-block:: python

   import algoseek_connector as ac

   manager = ac.ResourceManager()
   data_source = manager.create_data_source("clickhouse")
   group = data_source.groups.USEquityMarketData.fetch()
   dataset = group.datasets.TradeOnlyMinuteBar.fetch()

   stmt = (
      dataset.select()
      .where(dataset.TradeDate > "2015-01-01").
      .order_by(dataset.Volume)
      .limit(1000000)
   )

   df = dataset.fetch_dataframe(stmt)


Features:

-  Selecting columns and arbitrary expressions based on columns
-  Filtering by column value/column expression
-  Grouping by column(s).
-  Sorting by column(s).
-  All common arithmetic, logical operations on dataset columns and function
   application.
-  Fetching query results as native Python types or as pandas DataFrames.
-  Streaming query results.

Contents
========

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   tutorials/datasets
   tutorials/sql
   api

.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
