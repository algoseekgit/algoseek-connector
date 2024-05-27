Library utilities
=================

.. _library-utilities:

Connection to metadata services
-------------------------------

.. _DatasetAPIProvider:

DatasetAPIProvider
------------------

.. autoclass:: algoseek_connector.dataset_api.DatasetAPIProvider
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