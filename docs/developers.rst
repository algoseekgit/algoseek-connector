.. _developers:

Developers guide
================

.. _dev_getting_started:

Getting Started
---------------

The build system used is `Poetry <https://python-poetry.org/>`_. Are recipe for
development installation is included in the `Makefile`:

.. code-block:: shell

    make dev-install

Running this command will install the package, along with dependencies to
build the documentation and execute tests. ``pre-commit`` hooks are also
installed. In order to run integration tests, access to the different data
sources is required. Contact the :ref:`maintainers` to obtain the data sources'
credentials.

All contribution to the project should be made by creating feature branches and
creating a corresponding PR. Be sure to pass all unit and integration tests
before submitting a PR. All new features should be :ref:`tested <testing>` and documented.
Refer to their corresponding sections for more information.


.. _testing:

Testing
-------

`Pytest <https://docs.pytest.org/en/7.4.x/>`_ is used as the suite for testing.
Testing requires access to Algoseek metadata API, ArdaDB and S3 buckets.

**Metadata API**
    Set user/pass using the environment variables `ALGOSEEK_API_USERNAME` and
    `ALGOSEEK_API_PASSWORD`.

**ArdaDB**
    Set host address, port, user and password using the environment variables
    `ALGOSEEK_ARDADB_HOST`, `ALGOSEEK_ARDADB_USER` and `ALGOSEEK_ARDADB_PASSWORD`.

**S3**
    Set access to dataset buckets setting the variables `ALGOSEEK_AWS_PROFILE`
    to get the credentials from ~/.aws/credentials or set user/password using
    the `ALGOSEEK_AWS_ACCESS_KEY_ID` and `ALGOSEEK_AWS_SECRET_ACCESS_KEY`
    environment variables. Also, For writing to S3 buckets, access to the
    development buckets needs to be set using the environment variables
    `ALGOSEEK_DEV_AWS_ACCESS_KEY_ID` and `ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY`.

The library tests are grouped in three different types: integration, learning and
unit. Integration and learning tests need access external resources to run.
See the next section on how to setup access to resources to execute tests.

**Unit tests**

    Check basic library component functionality. This type of tests do not
    connect to external data sources such as DB or REST APIs. If necessary,
    external connections are mocked. Unit test are executed with the following
    command:

    .. code-block:: shell

        make unit-tests

**Integration tests**
    Check library functionality in real-world scenarios, connecting to external
    data sources. Usually they take longer to run and are not executed by
    default. Integration tests are executed with the following command:

    .. code-block:: shell

        make integration-tests

**Learning tests**

    This kind of tests do not test library functionality. They are created to
    learn how third party library works and check their behavior in different
    scenarios. They are not executed by default. Learning tests are executed
    with the following command:

    .. code-block:: shell

        make learning-tests

We aim to have 100 % code coverage on the library. A code coverage report is
executed with the following command:

.. code-block:: shell

    make coverage

.. _documentation:

Improving the documentation
---------------------------

The library documentation is generated using
`Sphinx <https://www.sphinx-doc.org/en/master/>`_.
The docstrings are written using the
`Numpy style <https://numpydoc.readthedocs.io/en/latest/>`_.
All public modules, classes, methods and functions **must** have a docstring.
Docstrings for private and magic functions/methods, but a brief description
of its usage is encouraged. Tutorials are recommended to explain intended usage
of the different facilities.

Communication Channels
----------------------

You can contact one of the project :ref:`maintainers` or check the project
discussions at GitHub.

Reporting an issue
------------------

COMPLETE

Versioning and Release Process
------------------------------

COMPLETE


Algoseek-connector architectural overview
-----------------------------------------

``algoseek-connector`` aims to provide a high-level, easy-to-use library to
fetch data from Algoseek datasets. In order to achieve this, we adhere to the
following principles:

- performance is a top priority.
- Maintain a minimum number of dependencies.
- Provide a unified interface to access datasets.
- Provide utilities to aid the user in dataset exploration.

Te overall architecture and functionality responds to these principles. The
following figures display the different modules of the library and the
architecture of the library API.

.. image:: _static/api-uml.png
    :target: _static/api-uml.png
    :alt: A UML diagram of the library API.


We can follow, in a top-bottom fashion, how the different classes are used by
an user:

**ResourceManager**
    The first point of contact of the user with the library. It lists and
    creates the different data sources. It basically a DataSource factory.
**DataSource**
    A data source manages the connection to a data source (ArdaDB or S3) and
    displays the different data groups. In other words, it orchestrates the
    process of requesting data. Below we discuss how to extend the library,
    creating new data sources.
**DataGroupMapping**
    A mapping class that stores a lightweight representation of data groups.
    It is used in the `groups` attribute of DataSource to display all available
    data groups at run time.
**DataGroupFetcher**
    A lightweight representation of a data group. It stores a description of
    the datagroup (see DataGroupDescription) and creates a DataGroup when
    requested.
**DataGroupDescription**
    A container class that stores the name and description of a data group.
**DataGroup**
    Manages a collection of related datasets.
**DataSetMapping**
    A mapping class that stores a lightweight representation of datasets. It is
    used in the `datasets` attribute of DataGroup to display all available
    datasets at run time.
**DataSetFetcher**
    A lightweight representation of a dataset. It stores a description of the
    dataset (see DataSetDescription). Provides functionality to download data
    from data set (in the case of S3 datasets) and to create a DataSet for
    querying data using SQL (ArdaDB).
**DataSetDescription**
    A container class that stores the name and description of a dataset, along
    with links to the dataset documentation and ColumnDescription.
**DataSet**
    A representation of a dataset using SQLAlchemy utilities. It provides
    functionality to fetch data from a dataset using SQL-like queries.

.. _maintainers:

Project maintainers
-------------------

- Gabriel Riquelme: gabrielr [at] algoseek [dot] com
- Taras Kuzyo: taras [at] algoseek [dot] com
