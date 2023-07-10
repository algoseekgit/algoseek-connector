.. _datasets:

Getting started
===============

The algoseek-connector library provide means to retrieve data from different
data sources in a straightforward way. To get started, first we will describe
the hierarchy used to organize data:

TODO: ADD DIAGRAM WITH CLASS HIERARCHY

The :py:class:`~algoseek_connector.manager.ResourceManager` is the first point of contact
to fetch data. It manages available data sources for an user:

.. code-block:: python

    import algoseek_connector as ac

    manager = ac.ResourceManager()

the :py:func:`~algoseek_connector.manager.ResourceManager.list_data_sources`
method produces a list of available data sources to connect to:

.. code-block:: python

    manager.list_data_sources()


For the following sections we will use the clickhouse data source, which can be
created with the
:py:func:`~algoseek_connector.manager.ResourceManager.create_data_source` method:

.. code-block:: python

    data_source = manager.create_data_source("clickhouse")


DataSources and DataGroups
--------------------------

A :py:class:`~algoseek_connector.base.DataSource` manages the connection to a
data source and allows access to data groups. A data group manages a collection
of related datasets. Thinking in terms of relational databases, a group is a
database. The available data groups can be retrieved by using the
:py:func:`~algoseek_connector.base.DataSource.list_datagroups` method:

.. code-block:: python

    data_source.list_data_groups()

Also, the `groups` attribute maintains a collection of
:py:class:`~algoseek_connector.base.DataGroup` available in a data source:

TODO: ADD GIF WITH AUTOCOMPLETION OF DATASETS.

A data group is created either by using the fetch method of the corresponding
group:

.. code-block:: python

    group = data_source.groups.USEquityData.fetch()

or by using the :py:func:`~algoseek_connector.base.DataSource.fetch_datagroup`
method:

.. code-block:: python

    group = data_source.fetch_datagroup("USEquityData")

In a similar way to data sources, data groups allows to list datasets:

.. code-block:: python

    group.list_datasets()

Once again, thinking in database terms, a dataset represents a table. Datasets
may be retrieved in two different ways: by using the
:py:func:`~algoseek_connector.base.DataGroup.fetch_dataset`:

.. code-block:: python

    dataset = group.fetch_dataset("TradeAndQuote")

or by using the fetch method of the corresponding dataset:

.. code-block:: python

    dataset = group.datasets.TradeAndQuote.fetch()

Datasets
--------

:py:class:`~algoseek_connector.base.DataSet` objects are a lightweight
representation of the data in a table, and act as an interface to retrieve data.

The :py:func:`~algoseek_connector.base.DataSet.head` method retrieves the
first rows from the dataset (5 by default) as a :py:class:`~pandas.DataFrame`:

.. code-block:: python

    dataset.head()

To fetch data from the dataset, the
:py:func:`~algoseek_connector.base.DataSet.select` method is used:

.. code-block:: python

    stmt = dataset.select().limit(10)
    data = dataset.fetch(stmt)

The first line creates a :py:class:`~sqlalchemy.sql.expression.Select` object.
Behind the scenes, Datasets are powered by the
`SQLAlchemy <https://www.sqlalchemy.org/>`_ library, enabling the creation of
complex SQL statements to retrieve data. Creating select statements is a topic
on its own. Refer to :ref:`this guide <sql>` for a detailed description on how
to create select statements.

In the second line, the select statement is used to retrieve data using the
:py:func:`~algoseek_connector.base.DataSet.fetch` method. The fetch method
retrieves data using Python native objects. Different alternatives are available
for retrieving data:

:py:func:`~algoseek_connector.base.DataSet.fetch`
    Fetch data using Python natives types.
:py:func:`~algoseek_connector.base.DataSet.fetch_iter`
    Stream data in chunks using Python native types.
:py:func:`~algoseek_connector.base.DataSet.fetch_dataframe`
    Fetch data as a :py:class:`pandas.DataFrame`.
:py:func:`~algoseek_connector.base.DataSet.fetch_iter_dataframe`
    Stream data in chunks using :py:class:`pandas.DataFrame`.