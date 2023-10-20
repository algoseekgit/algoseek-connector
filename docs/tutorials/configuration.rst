.. _configuration:

Configuration
=============

The default settings are managed through the
:py:class:`~algoseek_connector.config.Settings` class which centralizes default
and user-defined settings. There are four levels at which the configuration can
be edited and each each levels precedes the others:

1.  Manually passing parameters to functions/classes overrides all user/default
    settings.
2.  Values set using the :py:class:`~algoseek_connector.config.Settings` class
    override the settings file.
3.  Values set in the configuration file override values set in environment
    variables.
4.  Set environment variables (see the :ref:`available-settings`).


To pass parameters manually, refer to the :ref:`api`. In the rest of this guide
the other approaches are discussed. The table at the end of this guide list
which settings can be edited using environment variables.

The settings class is a container for default/user defined settings and
loads values from the :ref:`configuration-file` or environment variables.
Settings are grouped according to their function and can be modified at run time
by using the :py:func:`~algoseek_connector.config.SettingsField.set` method of
each setting field. For example, the following code modifies the host for ardadb:

.. code-block:: python

    import algoseek_connector as ac
    config = ac.Settings()
    new_host = "1.2.3.4"
    config.ardadb.credentials.host.set(new_host)

Note that some setting fields are not editable using this method and can only be
modified from the :ref:`configuration file <configuration-file>`.

.. _configuration-file:

The configuration file
----------------------

Using the configuration file is completely optional, but it allows to easily
modify the default behavior of the different data sources. The config file uses
the `TOML format <https://toml.io/>`_ and is located at
``~/.algoseek/config.toml``. To create a configuration file with default values,
the following command can be used:

.. code-block:: python

    import algoseek_connector as ac
    ac.config.create_config_file()

This creates the following file:

.. code-block:: toml

    [ardadb.credentials]
    # uncomment fields and edit fields
    # host = "0.0.0.0"
    # port = 8123
    # user = "default"
    # password = "default"

    [ardadb.settings]
    # Parameters supported by clickhouse_connect.get_client function
    # see https://clickhouse.com/docs/en/integrations/python#clickhouse-connect-driver-api

    [s3.credentials]
    # uncomment fields and edit fields
    # profile_name = "default"
    # aws_access_key_id = "default"
    # aws_secret_access_key = "default"

    [s3.quota]
    # values are in bytes
    download_limit = 1099511627776
    download_limit_do_not_modify = 21990232555520

    [s3.settings]
    # add setting from
    # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html


.. _available-settings:

Available settings
------------------

**ardadb.credentials**
    -   `host`: The ArdaDB host address. env: ``ALGOSEEK_ARDADB_HOST``. Editable
        at runtime: yes.
    -   `user`: The ArdaDB host port. env: ``ALGOSEEK_ARDADB_PORT``. Editable associated
        runtime : yes.
    -   `user`: The ArdaDB user name. env: ``ALGOSEEK_ARDADB_USERNAME``.
        Editable at runtime: yes.
    -   `password`: The ArdaDB user password. env: ``ALGOSEEK_ARDADB_PASSWORD``.
        Editable at runtime: yes.
**ardadb.settings**
    Any clickhouse client `optional settings <https://clickhouse.com/docs/en/integrations/python#clickhouse-connect-driver-api>`__.
    Editable at runtime: yes.
**s3.credentials**
    -   `profile_name`: A profile stored on `~/.aws/credentials`. If a profile
        name is provided, the `aws_access_key_id` and `aws_secret_access_key`
        values are ignored. env: ``ALGOSEEK_AWS_PROFILE``. Editable at runtime:
        yes.
    -   `aws_access_key_id`: The AWS access key associated with an IAM user or
        role. env: ``ALGOSEEK_AWS_ACCESS_KEY_ID``. Editable at runtime: yes.
    -   `aws_secret_access_key`: The secret key associated with the access key.
        env: ``ALGOSEEK_AWS_SECRET_ACCESS_KEY``. Editable at runtime: yes.
**s3.settings**
    Any boto3.Session `optional settings <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html>`_.
    Editable at runtime: yes.
**s3.quota**
    -   `download_limit`:  A download limit for S3 datasets. Set at 1 TiB by
        default. Editable at runtime: yes.
    -   `download_limit_edit_at_own_risk`: A second download limit for S3
        datasets. Only editable from the configuration file. Set at 20 TiB by
        default. Editable at runtime: no.

