.. _sql:

Creating SQL queries
********************

Here we describe how to create select statements using
:py:class:`~algoseek_connector.base.DataSet` objects. For instructions on how to
create datasets from different data sources please see :ref:`this guide <datasets>`.

The Dataset class is built on top of `SQLAlchemy <https://www.sqlalchemy.org/>`_
table and column constructs and supports most of the standard SQL operations.
Dataset objects do not store data and are just a mean to retrieve data from a
source by means of queries.

Before starting with the description of the query generation process, it is
important to make the following remark: a good understanding of the inner
working of the DBMS where the data is hosted is necessary to maximize the speed
at which data is requested. For example, in ArdaDB, which is implemented using
`Clickhouse <https://clickhouse.com/>`_, knowing how primary indexes are
implemented will result in better performance when querying the data.
See `this article <https://clickhouse.com/docs/en/optimize/sparse-primary-indexes#data-is-organized-into-granules-for-parallel-data-processing>`_
as a reference.

Now, we are ready to review the query generation process. First, we load a
dataset to use in the examples:

.. code-block:: python

    import algoseek_connector as ac

    manager = ac.ResourceManager()
    data_source = manager.create_data_source("ardadb")
    group = data_source.groups.USEquityMarketData.fetch()
    dataset = group.datasets.TradeOnlyMinuteBar.fetch()

Columns and simple select statements
------------------------------------

Each column of a dataset is represented as a :py:class:`sqlalchemy.Column` and
is accessed by using either attribute or dict-like access:

.. code-block:: python

  # access by attribute
  col = dataset.ASID

  # access by index
  col = dataset["ASID"]

The :py:class:`~algoseek_connector.base.ColumnHandle` class also allows access
to a dataset columns and it can be created using the
:py:func:`~algoseek_connector.base.DataSet.get_column_handle` method:

.. code-block:: python

    c = dataset.get_column_handle()

    # access by attribute
    c.ASID

    # access by index
    c["ASID"]

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
            FROM
                USEquityMarketData.TradeOnlyMinuteBar

The SQL tab displays the equivalent select statement generated. A subset
of columns can be selected by passing them as arguments:


.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            stmt = dataset.select(
                dataset.TradeDate,
                dataset.Ticker,
                dataset.Volume,
            )

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,
                USEquityMarketData.TradeOnlyMinuteBar.Volume
            FROM
                USEquityMarketData.TradeOnlyMinuteBar

It is often the case that we want to exclude a subset of columns from the query.
This is done by passing them as a sequence to the `exclude` parameter:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            exclude_columns = (dataset.ASID, dataset.TotalTrades)
            stmt = dataset.select(exclude=exclude_columns)

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,
                USEquityMarketData.TradeOnlyMinuteBar.BarDateTime,
                USEquityMarketData.TradeOnlyMinuteBar.FirstTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.HighTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.LowTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.LastTradePrice,
                USEquityMarketData.TradeOnlyMinuteBar.VolumeWeightPrice,
                USEquityMarketData.TradeOnlyMinuteBar.Volume,
            FROM
                USEquityMarketData.TradeOnlyMinuteBar


Filter rows with the WHERE clause
---------------------------------

SQLalchemy columns are overloaded to support comparison operators:

.. code-block:: python

    # examples of comparison operators
    col1 = dataset.HighTradePrice
    col2 = dataset.LowTradePrice,

    # equality
    col1 == col2

    # greater than
    col1 > col2

    # greater-or-equal than
    col1 >= col2

    # between
    value1 = 1
    value2 = 2
    col1.between(value1, value2)

    # in
    list_of_values = [1, 2, 3, 4]
    col1.in_(list_of_values)

Logical operator are also overloaded:

.. code-block:: python

    # AND
    col1 & col2

    # OR
    col1 | col2

    # NOT
    ~col1

A complete reference of operators is available
`here <https://docs.sqlalchemy.org/en/20/core/operators.html>`_.
Using these operators, the WHERE clause can be included using the
:py:func:`~sqlalchemy.Select.where` method of the Select construct. We
present here several commonly used examples of the WHERE clause:

Filter data using a ticker symbol:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            tickers = "ABC"
            stmt = dataset.select().where(dataset.Ticker == tickers)

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
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            WHERE
                USEquityMarketData.TradeOnlyMinuteBar.Ticker = 'ABC'

Filter data using a list of tickers:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            tickers = ["ABC", "DEF"]
            stmt = dataset.select().where(dataset.Ticker.in_(tickers))

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
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            WHERE
                USEquityMarketData.TradeOnlyMinuteBar.Ticker IN ('ABC', "DEF")

Filter data by date:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            date = "20230701"
            stmt = dataset.select().where(dataset.TradeDate = date)

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
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            WHERE
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate = "20230701"


Filter data by date range:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            start = "20230701"
            end = "20230710"
            stmt = dataset.select().where(dataset.TradeDate.between(start, end))

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
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            WHERE
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate BETWEEN "20230701" AND "20230710"



Filter data by date range and symbol:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            start = "20230701"
            end = "20230710"
            ticker = "ABC"
            cond = (
                dataset.TradeDate.between(start, end) &
                (dataset.Ticker == tickers)
            )
            stmt = dataset.select().where(cond)

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
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            WHERE
                (USEquityMarketData.TradeOnlyMinuteBar.TradeDate BETWEEN "20230701" AND "20230710")
                AND USEquityMarketData.TradeOnlyMinuteBar.Ticker = 'ABC'



Aggregating rows with GROUP BY
------------------------------

Row aggregation is available through the :py:func:`~sqlalchemy.Select.group_by`
method, which accepts a sequence of columns that are used perform the
aggregation. Arbitrary aggregation functions can be created with SQLAlchemy
``func`` function generator:

.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            from sqlalchemy import func

            c = dataset.get_column_handle()
            stmt = (
                dataset
                .select(
                    c.TradeDate,
                    c.Ticker,
                    func.avg(c.HighTradePrice).label("MeanHighPrice")
                )
                .group_by(c.TradeDate, c.Ticker)
            )

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,
                avg(USEquityMarketData.TradeOnlyMinuteBar.HighTradePrice) AS MeanHighPrice
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            GROUP BY
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,

Note that column aliasing is supported through the ``label`` method. It is
important to check the SQL reference to see which aggregate functions are
available. For ArdaDB, check the supported functions
`here <https://clickhouse.com/docs/en/sql-reference/aggregate-functions>`_.
The HAVING clause is also supported through the ``having`` method:


.. tab-set::

    .. tab-item:: Python

        .. code-block:: python

            from sqlalchemy import func

            c = dataset.get_column_handle()
            stmt = (
                dataset
                .select(
                    c.TradeDate,
                    c.Ticker,
                    func.min(c.HighTradePrice).label("MinHighPrice")
                )
                .group_by(c.TradeDate, c.Ticker)
                .having(func.min(c.HighTradePrice) > 1000.0)
            )

    .. tab-item:: SQL

        .. code-block:: sql

            SELECT
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,
                min(USEquityMarketData.TradeOnlyMinuteBar.HighTradePrice) AS MinHighPrice
            FROM
                USEquityMarketData.TradeOnlyMinuteBar
            GROUP BY
                USEquityMarketData.TradeOnlyMinuteBar.TradeDate,
                USEquityMarketData.TradeOnlyMinuteBar.Ticker,
            HAVING
                min(USEquityMarketData.TradeOnlyMinuteBar.HighTradePrice) > 1000.0