"""Base tools for the algoseek_connector library."""

from abc import ABC, abstractmethod
from sqlalchemy import MetaData, select, Select


class DataSet(ABC):
    """Retrieves data from a Dataset using SQL queries."""

    def __init__(self, group, table):
        self.group = group
        self._table = table
        self._query = None
        for column in table.c:
            setattr(self, column.name, column)

    def select(self, *args, **kwargs) -> Select:
        """Select data using chained methods with SQL-like syntax."""
        if args:
            # TODO: validate columns
            stmt = select(*args)
        else:
            stmt = select(self._table)
        self._stmt = stmt
        return stmt

    @abstractmethod
    def fetch(self):
        """Load the requested data."""

    @abstractmethod
    def get_function_handler(self):
        """Create a handler to available functions."""

    def get_column_handler(self):
        """Create a handler to Dataset columns."""
        return self._table.c


class DataGroup(ABC):
    """Manage a collection of related datasets."""

    def __init__(self) -> None:
        self.metadata = MetaData()

    def list_datasets(self) -> list[str]:
        """List available datasets."""

    def get_dataset(self, name: str) -> DataSet:
        """Load a dataset."""
