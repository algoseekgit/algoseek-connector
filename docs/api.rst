.. _api:

===============
 API Reference
===============

Here you can find the reference for the objects used by algoseek-connector
library:

.. _ResourceManager:

ResourceManager
---------------

.. autoclass:: algoseek_connector.manager.ResourceManager
    :members:

.. _DataSource:

DataSource
----------

.. autoclass:: algoseek_connector.base.DataSource
    :members:

.. _DataGroup:

DataGroup
---------

.. autoclass:: algoseek_connector.base.DataGroup
    :members:
    :undoc-members:

.. _DataSet:

DataSet
-------

.. autoclass:: algoseek_connector.base.DataSet
    :members:

.. _ColumnHandle:

ColumnHandle
------------

.. autoclass:: algoseek_connector.base.ColumnHandle
    :members:

.. _FunctionHandle:

FunctionHandle
--------------

.. autoclass:: algoseek_connector.base.FunctionHandle
    :members:


.. _LibraryUtilities:

Library utilities
=================

Connection to metadata services
-------------------------------

.. _AuthToken:

AuthToken
---------

.. autoclass:: algoseek_connector.metadata_api.AuthToken
    :members:

.. _BaseAPIConsumer:

BaseAPIConsumer
---------------

.. autoclass:: algoseek_connector.metadata_api.BaseAPIConsumer
    :members:


Client Protocols
----------------

Implementation of the ClientProtocol for the different data sources. These
classes should not be instantiated directly.

.. _ArdaDBClientProtocol:

ClickHouseClient
----------------

.. autoclass:: algoseek_connector.clickhouse.ClickHouseClient
    :members:

.. _S3ClientProtocol:

S3DownloaderClient
------------------

.. autoclass:: algoseek_connector.s3.S3DownloaderClient
    :members: