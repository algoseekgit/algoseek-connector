"""
Base tools for the algoseek-connector library.

Provides:

Classes
-------
DataSource
    Manage connection to a Database.
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
        groups = [DataGroupFetcher(self, x) for x in self._client.list_datagroups()]
        self.groups = DataGroupMapping(*groups)

    @property
    def client(self) -> ClientProtocol:
        """Get the data source client."""
        return self._client

    def fetch_datagroup(self, name: str) -> "DataGroup":
        """Retrieve a data group."""
        return self.groups[name].fetch()

    def list_datagroups(self) -> list[str]:
        """List available data groups."""
        return self.client.list_datagroups()


class DataGroupMapping:
    """
    Mapping class that stores DataGroups from a DataSource.

    Implements the Mapping protocol.
    """

    def __init__(self, *groups: "DataGroupFetcher"):
        for g in groups:
            setattr(self, g.name, g)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self) -> Generator[str, None, None]:
        yield from self.__dict__

    def __getitem__(self, key: str) -> "DataGroupFetcher":
        try:
            return self.__dict__[key]
        except KeyError:
            raise InvalidDataGroupName(key) from None

    def _ipython_key_completions_(self):  # pragma: no cover
        return list(self.__dict__)


class DataGroupFetcher:
    """PlaceHolder class to fetch DataGroups."""

    def __init__(self, source: DataSource, name: str) -> None:
        self._source = source
        self._name = name
        self._group = None

    @property
    def name(self) -> str:
        """Get the group name."""
        return self._name

    def fetch(self) -> "DataGroup":
        """Fetch the data group instance."""
        if self._group is None:
            group = DataGroup(self._source, self._name)
            self._group = group
        else:
            group = self._group
        return group


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
        self._client = source.client
        self._name = name
        self.metadata = MetaData()
        datasets = [DataSetFetcher(self, x) for x in self.list_datasets()]
        self.datasets = DataSetMapping(*datasets)

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
        InvalidDataSetName
            If an invalid dataset name is provided.

        """
        return self.datasets[name].fetch()

    def list_datasets(self) -> list[str]:
        """List available datasets."""
        return self.client.list_datasets(self.name)


class DataSetMapping:
    """
    Mapping class that stores DataGroups from a DataSource.

    Implements the Mapping protocol.
    """

    def __init__(self, *datasets: "DataSetFetcher"):
        for ds in datasets:
            setattr(self, ds.name, ds)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self) -> Generator[str, None, None]:
        yield from self.__dict__

    def __getitem__(self, key: str) -> "DataSetFetcher":
        try:
            return self.__dict__[key]
        except KeyError:
            raise InvalidDataSetName(key) from None

    def _ipython_key_completions_(self):  # pragma: no cover
        return list(self.__dict__)


class DataSetFetcher:
    """Placeholder class that fetch dataset."""

    def __init__(self, group: DataGroup, name: str):
        self._group = group
        self._name = name
        self._dataset = None

    @property
    def name(self) -> str:
        """Get the dataset name."""
        return self._name

    def fetch(self) -> "DataSet":
        """Fetch the dataset."""
        if self._dataset is None:
            group = self._group
            dataset_metadata = group.client.fetch_dataset_metadata(
                group.name, self.name
            )
            dataset = DataSet(self._group, dataset_metadata)
            self._dataset = dataset
        else:
            dataset = self._dataset
        return dataset


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

    def __init__(self, group: "DataGroup", table_metadata: DataSetMetadata):
        self._group = group
        self._name = table_metadata.name
        table_name = f"{group.name}.{table_metadata.name}"
        table = Table(table_name, group.metadata, *table_metadata.columns, quote=False)
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

    def __getitem__(self, key: str) -> Column:
        return self.c[key]

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

    def fetch(self, stmt: Select, **kwargs) -> dict[str, tuple]:
        """
        Fetch data using a select statement.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.
        kwargs :
            Optional parameters passed to the underlying ClientProtocol.fetch
            method.

        """
        query = self.client.compile(stmt)
        return self.client.fetch(query, **kwargs)

    def fetch_iter(
        self, stmt: Select, size: int, **kwargs
    ) -> Generator[dict[str, tuple], None, None]:
        """
        Stream data using a select statement.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.
        size : int
            The size of each data chunk.
        kwargs :
            Optional parameters passed to the underlying client
            fetch_iter method.

        Yields
        ------
        dict[str, tuple]
            A dictionary with column name/column data pairs.

        """
        query = self.client.compile(stmt)
        yield from self.client.fetch_iter(query, size, **kwargs)

    def fetch_dataframe(self, stmt: Select, **kwargs) -> DataFrame:
        """
        Fetch data using a select statement. Output columns as Pandas DataFrame.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.
        kwargs :
            Optional parameters passed to the underlying client
            fetch_dataframe method.

        Returns
        -------
        pandas.DataFrame

        """
        query = self.client.compile(stmt)
        return self.client.fetch_dataframe(query, **kwargs)

    def fetch_iter_dataframe(
        self, stmt: Select, size: int, **kwargs
    ) -> Generator[DataFrame, None, None]:
        """
        Stream data using a select statement. Output data as Pandas DataFrame.

        Parameters
        ----------
        stmt : Select
            A SQLAlchemy Select statement created using the select method.
        size : int
            The size of each data chunk.
        kwargs :
            Optional parameters passed to the underlying client
            fetch_iter_dataframe method.

        Yields
        ------
        pandas.DataFrame

        """
        query = self.client.compile(stmt)
        yield from self.client.fetch_iter_dataframe(query, size, **kwargs)

    def compile(self, stmt: Select) -> "CompiledQuery":
        """Compiles the statement into a dialect-specific SQL string."""
        return self.client.compile(stmt)

    def _repr_html_(self):  # pragma: no cover
        """Display the Dataset in jupyter notebooks using HTML."""
        desc = "test-description."
        return _make_dataset_html_description(self.name, desc, self.c)

    def _ipython_key_completions_(self):  # pragma: no cover
        """Add autocomplete integration for keys in Ipython/Jupyter."""
        return self.c._ipython_key_completions_()


@dataclass(frozen=True)
class DataSetMetadata:
    """
    Container class for table metadata.

    Attributes
    ----------
    name : str
        The dataset name.
    columns : list[Column]
        The dataset columns.

    """

    name: str
    columns: list[Column]


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

    def __len__(self) -> int:
        return len(self.__dict__)

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
    """Adapter interface for DB clients."""

    @abstractmethod
    def compile(self, stmt: Select) -> CompiledQuery:
        """Compile a SQLAlchemy Select statement into a CompiledQuery."""

    @abstractmethod
    def fetch_dataset_metadata(self, group: str, name: str) -> DataSetMetadata:
        """Create a dataset."""

    @abstractmethod
    def create_function_handle(self) -> FunctionHandle:
        """Create a FunctionHandle instance."""

    @abstractmethod
    def fetch(self, query: CompiledQuery, **kwargs) -> dict[str, tuple]:
        """Fetch a select query."""

    @abstractmethod
    def fetch_iter(
        self, query: CompiledQuery, size: int, **kwargs
    ) -> Generator[dict[str, tuple], None, None]:
        """Yield a select query in chunks."""

    @abstractmethod
    def fetch_dataframe(self, query: CompiledQuery, **kwargs) -> DataFrame:
        """Fetch a select query and output results as a Pandas DataFrame."""

    @abstractmethod
    def fetch_iter_dataframe(
        self, query: CompiledQuery, size: int, **kwargs
    ) -> Generator[DataFrame, None, None]:
        """Yield a select query in chunks, using pandas DataFrames."""

    @abstractmethod
    def list_datagroups(self) -> list[str]:
        """List available data groups."""

    @abstractmethod
    def list_datasets(self, group: str) -> list[str]:
        """List available data groups."""


def _make_dataset_html_description(
    name: str, description: str, column_handle: ColumnHandle
) -> str:
    table_content = _make_dataset_html_table(column_handle)
    html = f"<h2>{name}</h2>" f"<p>{description}</p>" f"{table_content}"
    return html


def _make_dataset_html_table(column_handle: ColumnHandle) -> str:
    rows = list()
    for col in column_handle:
        name = col.name
        t = str(col.type)
        description = col.doc
        column_html = f"<tr>\n<td>{name}</td><td>{t}</td><td>{description}</td></tr>"
        rows.append(column_html)
    html_rows = "\n".join(rows)
    table_header = "<tr>\n<th>Name</th><th>Type</th><th>Description</th></tr>"
    return f"<table>\n{table_header}\n{html_rows}\n</table>"
