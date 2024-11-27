Algoseek connector
==================

Algoseek-connector is a Python library that enables data retrieval from *algoseek* datasets by
using sql-like queries using method chaining. It provides utilities to:

-  Selecting columns and arbitrary expressions based on columns
-  Filtering by column value/column expression
-  Grouping by column(s).
-  Sorting by column(s).
-  All common arithmetic, logical operations on dataset columns and function
   application.
-  Fetching query results as native Python types or as pandas DataFrames.
-  Streaming query results.

.. code-block:: python

   import algoseek_connector as ac

   manager = ac.ResourceManager()
   data_source = manager.create_data_source("ArdaDB")
   group = data_source.groups.USEquityMarketData.fetch()
   dataset = group.datasets.TradeOnlyMinuteBar.fetch()

   stmt = (
      dataset
      .select()
      .where(dataset.TradeDate > "2015-01-01").
      .order_by(dataset.Volume)
      .limit(1000000)
   )

   df = dataset.fetch_dataframe(stmt)

Contents
--------

.. toctree::
   :maxdepth: 2

   algoseek_connector/tutorials
   algoseek_connector/api
