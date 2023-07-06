.. _datasets:

Getting started
===============

TODO: COMPLETE DOCS

The algoseek-connector library provide means to retrieve data from different
data sources in a straightforward way. To get started, first we will describe
the hierarchy used to organize data:

The :py:class:`~algoseek_connector.manager.ResourceManager` is the first point of contact
to fetch data. It manages available data sources for an user:

.. code-block:: python

    import algoseek_connector as ac

    manager = ac.ResourceManager()

the :py:func:`~algoseek_connector.manager.ResourceManager.list_data_sources` produces
a list of available data sources to connect to:

.. code-block:: python

    manager.list_data_sources()


For the following sections we will use the clickhouse data source:

.. code-block:: python

    data_source = manager.create_data_source("clickhouse")


DataSources and DataGroups
--------------------------

A :py:class:`~algoseek_connector.base.DataSource` manages the connection to a
data source and allows access to data groups. A data group manages a collection
of related datasets. Thinking in terms of relational databases, a group is a
database. The available data groups can be retrieved by using the
:py:func:`~algoseek_connector.base.DataSource.list_data_groups` method:

.. code-block:: python

    data_source.list_data_groups()

Also, the `groups` attribute maintains a collection of
:py:class:`~algoseek_connector.base.DataGroup` available in a data source:

TODO: ADD GIF WITH AUTOCOMPLETION OF DATASETS.

`groups` also support retrieving a data group by key.

.. code-block:: python

    group = data_source.get_data_group("USEquityData")


In a similar way to data sources, data groups allows to list datasets:

.. code-block:: python

    group.list_datasets()

Once again, thinking in database terms, a dataset represents a table.


Fetching datasets
-----------------

The first step to create a dataset is