.. _developers:

Developers guide
================

.. _dev_getting_started:

Getting Started
---------------

This library is developed for Python >= 3.10, with `Poetry <https://python-poetry.org/>`_ as the build
and dependency management tool. A development installation recipe is provided in the project's Makefile:

.. code-block:: shell

    make dev-install

Executing this command will install the package along with all dependencies required for building
documentation and running tests. Additionally, `pre-commit <https://pre-commit.com/>`_ hooks are configured
to enforce code quality standards. To execute integration tests, access to relevant data sources is necessary.
Please contact the project maintainers to obtain the appropriate credentials.

The library follows the `semantic versioning <https://semver.org/>`_ convention, using the format
MAJOR.MINOR.PATCH for releases. Releases are managed automatically by the release please GitHub action.
The version number is bumped automatically based on `conventional commit <https://www.conventionalcommits.org/en/v1.0.0/>`_
messages. To create a new release, the only required action is to merge the release please bot release branch.
Once a new release is created, the poetry publish action publish the package in the Python package index.

The `commitizen <https://commitizen-tools.github.io/commitizen/>`_ tool comes bundled with the development
dependencies. This tool should be used to generate conventional commit messages.

Contributions to the project should be made via feature branches, with corresponding pull requests (PRs).Ensure
that all unit and integration tests pass successfully before submitting a PR. Additionally, new features must be
:ref:`tested <testing>` and :ref:`documented <documentation>`. Refer to the respective sections in this guide for
detailed instructions on testing and documentation procedures.

.. _testing:

Testing
-------

The testing suite for this library is based on `Pytest <https://docs.pytest.org/en/stable/index.html>`_. Tests are
organized into three categories: unit tests, integration tests, and learning tests.

**Unit tests**

Unit tests verify the functionality of individual components within the library. These tests are isolated
from external systems, such as databases or REST APIs; external interactions are simulated using mocks
when necessary. Unit tests can be executed using the following command:

.. code-block:: shell

    make unit-tests

**Integration tests**

Integration tests evaluate the library's behavior in real-world scenarios, interacting with external data
sources (e.g., databases, APIs). These tests tend to have longer execution times and are not run by default.
To execute integration tests, use the following command:

.. code-block:: shell

    make integration-tests

**Learning tests**

Learning tests do not assess the library's functionality. Instead, they are designed to explore and evaluate
the behavior of third-party libraries in various scenarios. These tests are not executed by default. To run
the learning tests, use the following command:

.. code-block:: shell

    make learning-tests

Integration and learning tests need access to external resources to run. Access is set through environment
variables:

**ArdaDB**

Set host address, port, user and password using the environment variables ``ALGOSEEK_ARDADB_HOST``,
``ALGOSEEK_ARDADB_USER`` and ``ALGOSEEK_ARDADB_PASSWORD``.

**S3**

Set access to dataset buckets setting the variables ``ALGOSEEK_AWS_PROFILE`` to get the credentials
from ``~/.aws/credentials`` or set user/password using the ``ALGOSEEK_AWS_ACCESS_KEY_ID`` and
``ALGOSEEK_AWS_SECRET_ACCESS_KEY`` environment variables. Also, For writing to S3 buckets, access to
the development buckets needs to be set using the environment variables ``ALGOSEEK_DEV_AWS_ACCESS_KEY_ID``
and ``ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY``.

We aim to achieve 100 % code coverage on the code base. A code coverage report is executed with the
following command:

.. code-block:: shell

    make coverage

.. _documentation:

Improving the documentation
---------------------------

The library's documentation is generated using `Sphinx <https://www.sphinx-doc.org/en/master/>`_.
Doc building dependencies are installed with the following command:

```sh
poetry install --with docs
```

Docstrings are written in the `Numpy style <https://numpydoc.readthedocs.io/en/latest/>`_.

All public modules, classes, methods, and functions must include a docstring. While docstrings for private
functions and magic methods are not strictly required, it is strongly encouraged to provide at least a brief
description of their purpose and usage.

Additionally, tutorials are encouraged to illustrate the intended usage and best practices for utilizing the
library's features.

To generate the HTML documentation, navigate to the docs directory and execute the following command:

.. code-block:: shell

    make html

Deprecating Features
--------------------

Feature deprecations must be communicated to users with a warning, indicating the version in which the feature
will be removed (typically a major version change). Deprecations should be marked in the following ways:

-   In the issue tracker: Open an issue in the GitHub repository to announce the deprecation and its timeline.
-   In the code: Use the ``.. deprecated::`` directive in the relevant function or class docstring, indicating
    the version when the feature will be removed.
-   Provide alternatives: If available, recommend an alternative approach or feature in the deprecation message.

.. _algoseek-architecture:

Architecture overview
---------------------

``algoseek-connector`` aims to provide a fast, high-level, easy-to-use library to fetch data from Algoseek
datasets. In order to achieve this, we adhere to the following principles:

- performance is a top priority.
- The number of dependencies must be kept to a minimum.
- Provide a unified interface to access datasets.
- Provide utilities to aid the user in dataset exploration.

The overall architecture and functionality responds to these principles. The following figures display the
different modules of the library and the architecture of the library API.

.. image:: _static/api-uml.png
    :target: _static/api-uml.png
    :alt: A UML diagram of the library API.


We can follow, in a top-bottom fashion, how the different classes are used by an user:

**ResourceManager** is the first point of contact of the user with the library. It lists and creates the
different data sources. It is basically a DataSource factory.

A **DataSource** manages the connection to a data source (ArdaDB or S3) and displays the different data
groups. In other words, it orchestrates the process of requesting data. :ref:`Below <creating-new-data-sources>`
we discuss how to extend the library, by creating new data sources.

**DataGroupMappings** store a lightweight representation of data groups. It is used in the `groups` attribute of
DataSource to display all available data groups at run time.

A **DataGroupFetcher** is lightweight representation of a data group. It stores a description of the datagroup
and creates a DataGroup when requested.

A **DataGroupDescription** is a container class that stores the name and description of a data group.

**DataGroups** manages a collection of related datasets.

A **DataSetMapping** stores a lightweight representation of datasets. It is used in the `datasets` attribute of
DataGroup to display all available datasets at run time.

A **DataSetFetcher** is a lightweight representation of a dataset. It stores a description of the dataset and
provides functionality to download data from a dataset, in the case of S3 datasets, and to create a DataSet for
querying data using SQL, in the case of ArdaDB.

A **DataSetDescription** is a container class that stores the name and description of a dataset, along with links
to the dataset documentation and ColumnDescription.

A **DataSet** represents a dataset using SQLAlchemy utilities. It provides functionality to fetch data from a
dataset using SQL-like queries.

.. _creating-new-data-sources:

Creating new data sources
-------------------------

A :py:class:`~algoseek_connector.base.DataSource` is created using two components: a
:py:class:`~algoseek_connector.base.ClientProtocol` and a
:py:class:`~algoseek_connector.base.DescriptionProvider`. To create a new data source, both components
must be implemented.

The ``DescriptionProvider`` provides descriptions for the data groups and datasets available in a data source
and needs to implement three methods:

``get_data_group_description``
    Takes a data group name and returns a
    :py:class:`~algoseek_connector.base.DataGroupDescription`.
``get_dataset_description``
    Takes a data group name and a dataset name and returns
    :py:class:`~algoseek_connector.base.DataSetDescription`.
``get_columns_description``
    Takes a dataset name and returns a list of
    :py:class:`~algoseek_connector.base.ColumnDescription`.

The ``ClientProtocol`` manages the connection to the data. Depending on the subset of functionality required
for each data source, different methods needs to be implemented.

At a minimum, the ``list_data_groups`` and ``list_dataset`` method must be implemented, which return a
list of available data groups and datasets respectively.

For downloading data, the ``download`` method must be implemented. Check the signature of the method in
the source code.

For querying data using SQL-like constructs, several methods must be implemented. First, the ``get_dataset_columns``
creates SQLAlchemy Column constructs for a dataset and allows the creation of
:py:class:`~algoseek_connector.base.DataSet` instances. The `compile` method, which takes a SQLAlchemy Select
statement and returns a :py:class:`~algoseek_connector.base.CompiledQuery` must also be implemented. The
implementation of this method depends on the specific characteristics of the DB used, but it usually involves
compiling the Select statement using a SQLAlchemy Dialect.

Once these methods are implemented, the different ways to fetch data from a dataset can be implemented:

``fetch``
    Fetch data using a CompiledQuery and returns a dictionary where keys are
    column names and values are tuples containing values of each row.
``fetch_iter``
    The same as ``fetch`` but the data is yielded in even-sized chunks.
``fetch_dataframe``
    Fetch data using a CompiledQuery and returns a pandas DataFrame.
``fetch_iter_dataframe``
    The same as ``fetch_dataframe`` but the data is yielded in even-sized chunks.
``execute``
    Executes SQL queries passing statements as strings.
``store_to_s3``
    Stores the query results into an S3 object.
