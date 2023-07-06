"""
Base tools for the algoseek-connector library.

Provides:

Classes
-------
DataSource
    Manage connection to a Database.
DataGroupMapping
    Container class for the DataGroups in a DataSource.
DataGroup
    Container class for a collection of related Datasets.
DataSet
    Represents a Table in a Database. Allows to query data from a data source.
CompiledQuery
    Container class for a query created using a DataSet.
ColumnHandle
    Container class for the columns of a DataSet.
FunctionHandle
    Container class for functions allowed in a database query.
ClientProtocol
    Interface to connect to different databases.

Exceptions
----------
InvalidDataGroupName
    Exception raised when an invalid DataGroup is requested.
InvalidDataSetName
    Exception raised when an invalid DataSet is requested.

"""

from __future__ import annotations  # delayed annotations

from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator, Optional, Protocol, Sequence

from sqlalchemy import Column, MetaData, Table, func, select
from sqlalchemy.sql import Select

if TYPE_CHECKING:  # pragma: no cover
    from pandas import DataFrame


class DataSource:
    """
    Manage the connection to a data source.

    See :ref:`here <datasets>` for a guide on how to work with data sources.

    Attributes
    ----------
    groups : DataGroupMapping
        Maintains the collection of available DataGroups.


    Methods
    -------
    get_datagroup:
        Retrieve a data group from the data source.
    list_datagroups:
        List available data groups.

    """

    def __init__(self, client: ClientProtocol):
        self._client = client
        groups = [DataGroup(self, x) for x in self._client.list_datagroups()]
        self.groups = DataGroupMapping(*groups)

    @property
    def client(self) -> ClientProtocol:
        """Get the data source client."""
        return self._client

    def get_datagroup(self, name: str) -> "DataGroup":
        """Retrieve a data group."""
        return self.groups[name]

    def list_datagroups(self) -> list[str]:
        """List available data groups."""
        return self._client.list_datagroups()


class DataGroupMapping(Mapping):
    """
    Mapping class that stores DataGroups from a DataSource.

    Implements the Mapping protocol.
    """

    def __init__(self, *groups: "DataGroup"):
        for g in groups:
            setattr(self, g.name, g)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self) -> Generator[str, None, None]:
        yield from self.__dict__

    def __getitem__(self, key: str) -> "DataGroup":
        try:
            return self.__dict__[key]
        except KeyError:
            raise InvalidDataGroupName(key)


class DataGroup:
    """
    Manage a collection of related datasets.

    Parameters
    ----------
    source : DataSource
        The data source where the data groups belongs.
    name :  str
        The data group name.

    Methods
    -------
    fetch_dataset:
        Retrieves a dataset from the data source.
    list_datasets:
        List available datasets.

    """

    def __init__(self, source: "DataSource", name: str) -> None:
        self._name = name
        self.metadata = MetaData()
        self._datasets: dict[str, DataSet] = dict()
        self._client = source.client

    @property
    def client(self) -> ClientProtocol:
        """Get the data source client."""
        return self._client

    @property
    def name(self) -> str:
        """Get the data group name."""
        return self._name

    def fetch_dataset(self, name: str) -> "DataSet":
        """
        Load a dataset from a data source.

        Parameters
        ----------
        name : str
            The dataset name.

        Raises
        ------
        ValueError
            If an invalid dataset name is provided.

        """
        if name not in self.list_datasets():
            raise InvalidDataSetName(name)

        if name not in self._datasets:
            table = self.client.create_dataset_table(self, name)
            self._datasets[name] = DataSet(self, table)

        return self._datasets[name]

    def list_datasets(self) -> list[str]:
        """List available datasets."""
        return self.client.list_datasets(self.name)


class DataSet:
    """
    Retrieve data from a data source using SQL queries.

    See :ref:`here <datasets>` for a detailed description on how work with
    datasets.

    Parameters
    ----------
    group : DataGroup
        The data group where the dataset will be included.
    table : sqlalchemy.Table
        Store table name and columns.

    Methods
    -------
    compile:
        Convert a sqlalchemy.Select statement into a CompiledQuery.
    fetch:
        Retrieve data from the data source.
    fetch_dataframe:
        Retrieve data from the data source as a pandas DataFrame.
    fetch_iter:
        Retrieve data in chunks from the data source.
    get_function_handle:
        Create a FunctionHandle object.
    get_column_handle:
        Create a column handle object.
    select:
        Build a sqlalchemy.Select statement using method chaining.

    """

    def __init__(self, group: "DataGroup", table: Table):
        self._group = group
        self._name = table.name.split(".")[-1]  # remove DB name "DBName.TableName"
        self._client = group.client
        self._table = table
        for column in table.c:
            setattr(self, column.name, column)
        self.c = ColumnHandle(table)

    @property
    def name(self) -> str:
        """Get the dataset name."""
        return self._name

    @property
    def group(self) -> DataGroup:
        """Get the dataset group."""
        return self._group

    @property
    def client(self) -> ClientProtocol:
        """Get the data source client."""
        return self._client

    def _repr_html_(self):  # pragma: no cover
        """Display the Dataset in jupyter notebooks using HTML."""
        from pandas import DataFrame

        d = dict()
        d["name"] = [x.name for x in self._table.columns]
        d["type"] = [str(x.type) for x in self._table.columns]
        df = DataFrame(d)
        df = df.set_index("name")
        return f"<h3>Dataset: {self.name}</h3>\n{df._repr_html_()}"

    def __getitem__(self, key: str) -> Column:
        return self.c[key]

    def _ipython_key_completions_(self):  # pragma: no cover
        """Add autocomplete integration for keys in Ipython/Jupyter."""
        return self.c._ipython_key_completions_()

    def get_column_handle(self) -> "ColumnHandle":
        """Get a handler object for fast access to dataset columns."""
        return ColumnHandle(self._table)

    def get_function_handle(self) -> "FunctionHandle":
        """Get a handle for fast access to supported functions."""
        return self.client.create_function_handle()

    def select(
        self, *args: Column, exclude: Optional[Sequence[Column]] = None
    ) -> Select:
        """
        Create a select statement using chained methods with SQL-like syntax.

        See :ref:`here <sql>` for a detailed guide on how to create select
        statements.

        Parameters
        ----------
        args : tuple of Columns
            Sequence of columns included in the select statement. If no columns
            are provided, use all columns in the dataset.
        exclude : sequence of Columns or None, default=None
            List of columns to exclude from the select statement.

        Returns
        -------
        :py:class:`sqlalchemy.sql.selectable.Select`

        """
        if args:
            columns = list(args)
        else:
            columns = [x for x in self.c]

        if exclude is not None:
            exclude_names = [x.name for x in exclude]
            columns = [x for x in columns if x.name not in exclude_names]

        if not columns:
            msg = "At least one column must be selected to create a select statement."
            raise ValueError(msg)

        return select(*columns)

    def fetch(self, stmt: Select) -> dict[str, tuple]:
        """
        Fetch data using a select statement.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.

        """
        query = self.client.compile(stmt)
        return self.client.fetch(query)

    def fetch_iter(
        self, stmt: Select, size: int
    ) -> Generator[dict[str, tuple], None, None]:
        """
        Fetch data using a select statement. Output columns as Numpy arrays.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.

        """
        query = self.client.compile(stmt)
        yield from self.client.fetch_iter(query, size)

    def fetch_dataframe(self, stmt: Select) -> DataFrame:
        """
        Fetch data using a select statement. Output columns as Pandas DataFrame.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.

        """
        query = self.client.compile(stmt)
        return self.client.fetch_dataframe(query)

    def compile(self, stmt: Select) -> "CompiledQuery":
        """Compiles the statement into a dialect-specific SQL string."""
        return self.client.compile(stmt)


class InvalidDataGroupName(KeyError):
    """Exception raised when an invalid DataGroup name is passed."""

    pass


class InvalidDataSetName(KeyError):
    """Exception raised when an invalid DataSet name is passed."""

    pass


@dataclass(frozen=True)
class CompiledQuery:
    """
    Container class for compiled queries.

    Attributes
    ----------
    sql : str
        Parametrized SQL statement.
    parameters : str
        Query parameters.

    """

    sql: str
    parameters: dict

    def _repr_html_(self):  # pragma: no cover
        """Display query as a code block in Jupyter notebooks."""
        from pygments import highlight
        from pygments.formatters import HtmlFormatter
        from pygments.lexers import get_lexer_by_name

        fmt = HtmlFormatter()
        lexer = get_lexer_by_name("SQL")
        style = "<style>{}</style>".format(fmt.get_style_defs(".output_html"))
        return style + highlight(self.sql, lexer, fmt)


class ColumnHandle:
    """
    Handle for fast access to a dataset columns.

    Support access to a dataset columns by attribute or by key.

    See :ref:`here <datasets>` for a guide on how to use column handles.

    """

    def __init__(self, table: Table):
        for column in table.c:
            setattr(self, column.name, column)

    def __getitem__(self, key: str) -> Column:
        return self.__dict__[key]

    def __iter__(self) -> Generator[Column, None, None]:
        for key in self.__dict__:
            yield self[key]

    def _ipython_key_completions_(self):  # pragma: no cover
        return self.__dict__.keys()


class FunctionHandle:
    """
    Handle for SQL functions.

    See :ref:`here <datasets>` for a guide on how to use function handles.

    """

    def __init__(self, function_names: list[str]):
        for f in function_names:
            setattr(self, f, getattr(func, f))


class ClientProtocol(Protocol):
    """Interface for DB connection used by the DataSource class."""

    @abstractmethod
    def compile(self, stmt: Select) -> CompiledQuery:
        """Compile a SQLAlchemy Select statement into a CompiledQuery."""

    @abstractmethod
    def create_dataset_table(self, group: DataGroup, name: str) -> Table:
        """Create a dataset."""

    @abstractmethod
    def create_function_handle(self) -> FunctionHandle:
        """Create a FunctionHandle instance."""

    @abstractmethod
    def fetch(self, query: CompiledQuery) -> dict[str, tuple]:
        """Fetch a select query."""

    @abstractmethod
    def fetch_iter(
        self, query: CompiledQuery, size: int
    ) -> Generator[dict[str, tuple], None, None]:
        """Yield a select query in chunks."""

    @abstractmethod
    def fetch_dataframe(self, query: CompiledQuery) -> DataFrame:
        """Fetch a select query and output results as a Pandas DataFrame."""

    @abstractmethod
    def fetch_iter_dataframe(
        self, query: CompiledQuery, size: int
    ) -> Generator[DataFrame, None, None]:
        """Yield a select query in chunks, using pandas DataFrames."""

    @abstractmethod
    def list_datagroups(self) -> list[str]:
        """List available data groups."""

    @abstractmethod
    def list_datasets(self, group: str) -> list[str]:
        """List available data groups."""
