"""Base tools for the algoseek_connector library."""

import numpy as np
from abc import ABC, abstractmethod
from sqlalchemy import func, Column, MetaData, select, Table
from sqlalchemy.sql import Select
from typing import Optional, Sequence
from pandas import DataFrame


class ColumnHandle:
    """Handle for fast access to a dataset columns."""

    def __init__(self, table: Table):
        for column in table.c:
            setattr(self, column.name, column)


class FunctionHandle:
    """Handle for supported functions."""

    def __init__(self, function_names: list[str]):
        for f in function_names:
            setattr(self, f, getattr(func, f))


class DataSet:
    """Retrieves data from a Dataset using SQL queries."""

    def __init__(self, group: "DataGroup", table: Table, source: "DataResource"):
        self.group = group
        self.name = table.name.split(".")[-1]  # remove DB name "DBName.TableName"
        self._source = source
        self._table = table
        for column in table.c:
            setattr(self, column.name, column)
        group.add_dataset(self)

    def get_column_handle(self) -> ColumnHandle:
        """Get a handler object for fast access to dataset columns."""
        return ColumnHandle(self._table)

    def get_function_handle(self) -> FunctionHandle:
        """Get a handle for fast access to supported functions."""
        return self._source.get_function_handle()

    def select(
        self, *args: Column, exclude: Optional[Sequence[Column]] = None
    ) -> Select:
        """
        Create a select statement using chained methods with SQL-like syntax.

        Parameters
        ----------
        args : Column
            Sequence of columns included in the select statement. If no columns
            are provided, use all columns in the dataset.
        exclude : Sequence[Column] or None, default=None
            List of columns to exclude from the select statement.

        """
        if args:
            columns = args
        else:
            columns = [x for x in self._table.columns]

        if exclude is not None:
            exclude_names = [x.name for x in exclude]
            columns = [x for x in columns if x.name not in exclude_names]

        if not columns:
            msg = "Cannot perform select if all column are excluded."
            raise ValueError(msg)

        return select(*columns)

    def fetch(self, stmt: Select) -> dict[str, list]:
        """
        Fetch data using a select statement.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.

        """
        return self._source.fetch(stmt)

    def fetch_numpy(self, stmt: Select) -> dict[str, np.ndarray]:
        """
        Fetch data using a select statement. Output columns as Numpy arrays.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.

        """
        return self._source.fetch_numpy(stmt)

    def fetch_dataframe(self, stmt: Select) -> DataFrame:
        """
        Fetch data using a select statement. Output columns as Pandas DataFrame.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.

        """
        return self._source.fetch_dataframe(stmt)


# TODO: reorganize creation of datasets and inclusion in data group.
class DataGroup:
    """Manage a collection of related datasets."""

    def __init__(self, source: "DataResource") -> None:
        self.metadata = MetaData()
        self.datasets: dict[str, DataSet] = dict()
        self._source = source

    def add_dataset(self, dataset: DataSet):
        """Add dataset to data group."""
        self.datasets[dataset.name] = dataset

    def get_dataset(self, name: str) -> Optional[DataSet]:
        """Get a dataset using the name."""
        return self.datasets.get(name)

    @abstractmethod
    def list_datasets(self) -> list[str]:
        """List available datasets."""


class DataResource(ABC):
    """Base class to manage a data source."""

    @abstractmethod
    def fetch(self, stmt: Select) -> dict[str, list]:
        """Fetch data and output using Python native types."""

    @abstractmethod
    def fetch_dataframe(self, stmt: Select) -> DataFrame:
        """Fetch data and output using Pandas DataFrame."""

    @abstractmethod
    def fetch_numpy(self, stmt: Select) -> dict[str, np.ndarray]:
        """Fetch data and output using Numpy arrays."""

    @abstractmethod
    def list_groups(self) -> list[str]:
        """List available data groups."""

    @abstractmethod
    def list_datasets(self, group: str) -> list[str]:
        """List available datasets in a group."""

    @abstractmethod
    def get_dataset(self, group: str, name: str) -> DataSet:
        """Get a dataset."""

    @abstractmethod
    def get_function_handle(self) -> FunctionHandle:
        """Create a FunctionHandler instance."""

    @abstractmethod
    def compile(self, stmt: Select) -> str:
        """Compiles the statement into a SQL string."""
