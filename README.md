# algoseek-connector

A wrapper library for ORM-like SQL builder and executor.
The library provides a simple pythonic interface to algoseek datasets with custom data filtering/selection.

## Supported Features

The following query operations on datasets are supported:
- Selecting columns and arbitrary expressions based on columns
- Filtering by column value/column expression
- Grouping by column(s)
- Sorting by column(s)
- All common arithmetic, logical operations on dataset columns and function application
- Fetching query results as a pandas DataFrame

## Installation

`algoseek-connector` is available on the Python Package Index. Install it using
the `pip` command:

    pip install algoseek-connector

## Documentation

Documentation is available on `ADD-LINK-WHEN-AVAILABLE`.

## Dev installation

`algoseek-connector` is installed using [Poetry](https://python-poetry.org/docs/#installation).

A Makefile recipe is available to install the package in developer mode along
with developer dependencies:

```sh
make dev-install
```

If `make` is not available, run:

    poetry install --with dev,docs
    pre-commit install

## Testing

Refer to the README inside the tests directory.

# Building the docs

The documentation is generated using the sphinx library. First, install
the necessary dependencies with the following command:

```sh
poetry install --with docs
```

Build the documentation using the Makefile located in the `docs` directory:

```sh
make html
```