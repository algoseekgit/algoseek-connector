.. _configuration:

Configuration
=============

The default settings are managed by the :py:class:`~algoseek_connector.settings.AlgoseekConnectorSettings`
class which centralizes default and user-defined settings. There are three levels at which
the configuration can be edited and each each levels precedes the others:

1.  Manually passing parameters to functions/classes.
2.  Values set using the :py:class:`~algoseek_connector.settings.AlgoseekConnectorSettings`.
3.  Values set in environment variables.

To pass parameters manually to the different functions, please refer to the :ref:`api`.
In the rest of this guide the other two approaches are discussed.


The following code snippet uses the :py:class:`~algoseek_connector.settings.AlgoseekConnectorSettings`,
to set the ArdaDB credentials:

.. code-block:: python

    from algoseek_connector.settings import load_settings

    settings = load_settings()

    # setting ArdaDB credentials
    settings.ardadb.user = "username"
    settings.ardadb.password = "password"

Alternatively, the same information can be set as environment variables:

.. code-block:: bash

    export ALGOSEEK__ARDADB__USER="username"
    export ALGOSEEK__ARDADB__PASSWORD="password"

Environment variable names have the following structure: ``ALGOSEEK__{SETTINGS_GROUP}__{SETTINGS_FIELD}``,
where `SETTINGS_GROUP` may be `ARDADB`, `DATASET_API` or `S3`. Refer to each :ref:`submodel API reference <settings-api>`
for the corresponding setting fields available.

