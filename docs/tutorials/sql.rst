.. _sql:

Creating SQL queries
********************

Here we describe how to create select statements using
:py:class:`~algoseek_connector.base.DataSet` objects. For instructions on how to
create datasets from different data sources please see :ref:`here <datasets>`.

The Dataset class is built on top of `SQLAlchemy <https://www.sqlalchemy.org/>`_
Tables and Columns constructs and support most of the standard SQL operations.
Also, depending on the data source, extra functionality may be available.

It is important to point out that Dataset objects do not store data and are
just a mean to retrieve data from a source by means of queries.

First, we load a dataset:

.. code-block:: python

  import algoseek_connector as ac

    manager = ac.ResourceManager()
    data_source = manager.create_data_source("clickhouse")
    group = data_source.groups.USEquityMarketData.fetch()
    dataset = group.datasets.TradeOnlyMinuteBar.fetch()

    dataset.head()  # check connection

Columns and simple select statements
------------------------------------

Each column of a dataset is represented as a :py:class:`sqlalchemy.Column` and
is accessed by using either attribute or dict-like access:

.. code-block:: python

  # access by attribute
  col = dataset.ASID

  # access by index
  col = dataset["ASID"]

The columns of a dataset allow us to build select statements using the
:py:func:`~algoseek_connector.base.DataSet.select` method:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            stmt = dataset.select()

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.BarDateTime,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,
                USEquityMarketData.TradeOnlyMinuteBar.ASID,
                USEquityMarketData.TradeOnlyMinuteBar.FirstTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.HighTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.LowTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.LastTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.VolumeWeightPrice,
                USEquityMarketData.TradeOnlyMinuteBar.Volume,
                USEquityMarketData.TradeOnlyMinuteBar.TotalTrades
            FROM USEquityMarketData.TradeOnlyMinuteBar

We can see in the SQL tab the equivalent select statement generated. A subset
of columns can bse selected by passing them as arguments:


.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            stmt = dataset.select(dataset.ASID, dataset.Ticker)

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.ASID,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker
            FROM USEquityMarketData.TradeOnlyMinuteBar

It is often the case that we want to exclude a subset of columns from the query.
This is done by passing them as a sequence to the `exclude` parameter:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            exclude_columns = (dataset.ASID, dataset.Ticker)
            stmt = dataset.select(exclude=exclude_columns)

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.BarDateTime,
                USEquityMarketData.TradeOnlyMinuteBar.FirstTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.HighTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.LowTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.LastTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.VolumeWeightPrice,
                USEquityMarketData.TradeOnlyMinuteBar.Volume,
                USEquityMarketData.TradeOnlyMinuteBar.TotalTrades
            FROM USEquityMarketData.TradeOnlyMinuteBar


