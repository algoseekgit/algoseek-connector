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

import datetime
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Union

from sqlalchemy import Column, MetaData, Table, func, select
from sqlalchemy.sql import Select

if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path
    from typing import Generator, Optional, Sequence

    from pandas import DataFrame

date_like = Union[datetime.date, str]


class DataSource:
    """
    Manage the connection to a data source.

    See :ref:`here <datasets>` for a guide on how to work with data sources.

    Attributes
    ----------
    client : ClientProtocol
        Provide the connection to the actual data source.
    description_provider : DescriptionProvider
        Provide descriptions and metadata for data groups and datasets.
    groups : DataGroupMapping
        Maintain the collection of available DataGroups.

    Methods
    -------
    fetch_datagroup:
        Retrieve a data group from the data source.
    list_datagroups:
        List available data groups.

    """

    def __init__(
        self, client: ClientProtocol, description_provider: DescriptionProvider
    ):
        self.description_provider = description_provider
        self.client = client
        groups = [DataGroupFetcher(self, x) for x in self.client.list_datagroups()]
        self.groups = DataGroupMapping(*groups)

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
            setattr(self, g.description.name, g)

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
    """Placeholder class that fetch DataGroups."""

    def __init__(self, source: DataSource, name: str) -> None:
        self._source = source
        self._description = source.description_provider.get_datagroup_description(name)
        self._group = None

    @property
    def description(self) -> DataGroupDescription:
        """Get the group name."""
        return self._description

    def fetch(self) -> "DataGroup":
        """Fetch the data group instance."""
        if self._group is None:
            group = DataGroup(self._source, self.description)
            self._group = group
        else:
            group = self._group
        return group

    def _repr_html_(self):  # pragma: no cover
        """Display the DataGroup in jupyter notebooks using HTML."""
        return self.description.html()


class DataGroup:
    """
    Manage a collection of related datasets.

    Parameters
    ----------
    source : DataSource
        The data source where the data groups belongs.
    description :  DataGroupDescription
        The data group description.

    Methods
    -------
    fetch_dataset:
        Retrieves a dataset from the data source.
    list_datasets:
        List available datasets.

    """

    def __init__(self, source: DataSource, description: DataGroupDescription) -> None:
        self._source = source
        self._description = description
        self.metadata = MetaData()
        datasets = [DataSetFetcher(self, x) for x in self.list_datasets()]
        self.datasets = DataSetMapping(*datasets)

    @property
    def description(self) -> DataGroupDescription:
        """Get the data group description."""
        return self._description

    @property
    def source(self) -> DataSource:
        """Get the data source."""
        return self._source

    def fetch_dataset(self, name: str) -> DataSet:
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
        return self._source.client.list_datasets(self.description.name)

    def _repr_html_(self):  # pragma: no cover
        """Display the DataGroup in jupyter notebooks using HTML."""
        return self.description.html()


class DataSetMapping:
    """
    Mapping class that stores Datasets from a DataGroup.

    Implements the Mapping protocol.
    """

    def __init__(self, *datasets: DataSetFetcher):
        for ds in datasets:
            setattr(self, ds.description.name, ds)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self) -> Generator[str, None, None]:
        yield from self.__dict__

    def __getitem__(self, key: str) -> DataSetFetcher:
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
        self._source = group.source
        group_name = self.group.description.name
        self._description = group.source.description_provider.get_dataset_description(
            group_name, name
        )
        self._dataset = None

    @property
    def description(self) -> DataSetDescription:
        """Get the dataset name."""
        return self._description

    @property
    def group(self) -> DataGroup:
        """Get the dataset group."""
        return self._group

    @property
    def source(self) -> DataSource:
        """Get the data source."""
        return self._source

    def download(
        self,
        download_path: Path,
        date: Union[date_like, tuple[date_like, date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Union[date_like, tuple[date_like, date_like], None] = None,
    ):
        """
        Download data from the dataset.

        Parameters
        ----------
        download_path : pathlib.Path
            Path to a directory to download dataset files.
        date : str, datetime.date or tuple
            Download data in this date range. Dates can be passed as a str with
            `yyyymmdd` format or as date objects. If a tuple is passed, it is
            interpreted as a date range and all dates in the closed interval
            between the two dates are generated. If a single date is passed,
            download data from this specific date.
        symbols : str or list[str]
            Download data associated with these symbols.
        expiration date : str, datetime.date, tuple or None, default=None
            Download data with expiration dates in this date range. Dates must
            be passed using the same format used for the `date` parameter.

        """
        self.source.client.download(
            self.description.name, download_path, date, symbols, expiration_date
        )

    def fetch(self) -> DataSet:
        """
        Create a dataset instance.

        DataSet allow to fetch data using SQL-like queries. See
        :ref:`here <datasets>` for a detailed description on how work with
        datasets.

        """
        if self._dataset is None:
            dataset = DataSet(self.group, self.description)
            self._dataset = dataset
        else:
            dataset = self._dataset
        return dataset

    def _repr_html_(self):  # pragma: no cover:
        """Display the Dataset in jupyter notebooks using HTML."""
        return self.description.html()


class DataSet:
    """
    Retrieve data from a data source using SQL queries.

    See :ref:`here <datasets>` for a detailed description on how work with
    datasets.

    Attributes
    ----------
    c : ColumnHandle
        A handle object for dataset columns.
    description : DataSetDescription
        The dataset description.
    group : DataGroup
        The data group where the dataset will be included.
    source : DataSource
        The data source of the dataset.

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

    def __init__(self, group: DataGroup, description: DataSetDescription):
        self._group = group
        self._description = description
        self._source = group.source
        group_name = group.description.name
        dataset_name = description.name
        table_name = f"{group_name}.{dataset_name}"
        columns = self.source.client.get_dataset_columns(group_name, dataset_name)
        table = Table(table_name, group.metadata, *columns, quote=False)
        self._table = table
        for column in table.c:
            setattr(self, column.name, column)
        self.c = ColumnHandle(table)

    @property
    def description(self) -> DataSetDescription:
        """Get the dataset name."""
        return self._description

    @property
    def group(self) -> DataGroup:
        """Get the dataset group."""
        return self._group

    @property
    def source(self) -> DataSource:
        """Get the data source client."""
        return self._source

    def __getitem__(self, key: str) -> Column:
        return self.c[key]

    def get_column_handle(self) -> ColumnHandle:
        """Get a handler object for fast access to dataset columns."""
        return ColumnHandle(self._table)

    def get_function_handle(self) -> FunctionHandle:
        """Get a handle for fast access to supported functions."""
        return self.source.client.create_function_handle()

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
        query = self.source.client.compile(stmt)
        return self.source.client.fetch(query, **kwargs)

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
        query = self.source.client.compile(stmt)
        yield from self.source.client.fetch_iter(query, size, **kwargs)

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
        query = self.source.client.compile(stmt)
        return self.source.client.fetch_dataframe(query, **kwargs)

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
        query = self.source.client.compile(stmt)
        yield from self.source.client.fetch_iter_dataframe(query, size, **kwargs)

    def compile(self, stmt: Select) -> CompiledQuery:
        """Compiles the statement into a dialect-specific SQL string."""
        return self.source.client.compile(stmt)

    def _repr_html_(self):  # pragma: no cover
        """Display the Dataset in jupyter notebooks using HTML."""
        return self.description.html()

    def _ipython_key_completions_(self):  # pragma: no cover
        """Add autocomplete integration for keys in Ipython/Jupyter."""
        return self.c._ipython_key_completions_()


@dataclass
class ColumnDescription:
    """
    Store column metadata from a dataset.

    Returns
    -------
    name : str
        The column name.
    type : str
        The column type.
    description : str
        The column description

    """

    name: str
    type: str
    description: str

    def get_type_name(self) -> str:
        """Get the type name."""
        ind = self.type.find("(")
        if ind == -1:  # no args
            type_name = self.type
        else:
            type_name = self.type[:ind]
        return type_name

    def get_type_args(self) -> list[str]:
        """Get the type arguments."""
        open_ind = self.type.find("(")
        if open_ind != -1:
            close_ind = -1
            type_args = [
                x.strip() for x in self.type[open_ind + 1 : close_ind].split(",")
            ]
        else:
            type_args = list()
        return type_args

    def html(self) -> str:  # pragma: no cover
        """Create a description of the column as an HTML row."""
        name = self.name
        t = self.type
        description = self.description
        return f"<tr>\n<td>{name}</td><td>{t}</td><td>{description}</td></tr>"


class DataSetDescription:
    """
    Store data used to create dataset instances.

    Attributes
    ----------
    name : str
        The dataset name.
    group : str
        The datagroup name.
    description : str
        The dataset description
    columns : list[ColumnMetadata] or None, default=None
        The dataset columns.

    """

    def __init__(
        self,
        name: str,
        group: str,
        columns: list[ColumnDescription],
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        self.name = name
        self.group = group
        self.columns = columns
        if display_name is None:
            display_name = name
        self.display_name = display_name
        if description is None:
            description = ""
        self.description = description

    def get_table_name(self) -> str:
        """Get the table name in the format `group.name`."""
        return f"{self.group}.{self.name}"

    def __repr__(self):  # pragma: no cover
        return f"DataSetDescription(name={self.name}, group={self.group}, columns={self.columns})"

    def html(self) -> str:  # pragma: no cover
        """Create an HTML description of the dataset."""
        rows = list()
        for c in self.columns:
            rows.append(c.html())
        html_rows = "\n".join(rows)
        table_header = "<tr>\n<th>Name</th><th>Type</th><th>Description</th></tr>"
        table_html = f'<table style="width:66%">\n{table_header}\n{html_rows}\n</table>'

        html = (
            f"<h2>{self.display_name}</h2>" f"<p>{self.description}</p>" f"{table_html}"
        )
        return html


class DataGroupDescription:
    """
    Container class for datagroup metadata.

    Attributes
    ----------
    name : str
        The data group name.
    display_name : str
        Name used for pretty print.
    description : str
        The data group description.

    """

    def __init__(self, name: str, description: str, display_name: Optional[str] = None):
        self.name = name
        self.description = description
        if display_name is None:
            self.display_name = name
        else:
            self.display_name = display_name

    def __str__(self) -> str:  # pragma: no cover
        return f"DataGroupDescription({self.name})"

    def html(self) -> str:  # pragma: no cover
        """Create an HTML description of the data group."""
        return f"<h2>{self.display_name}</h2>" f"<p>{self.description}</p>"


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


class DescriptionProvider(Protocol):
    """Provide descriptions for datagroups, datasets and columns."""

    @abstractmethod
    def get_datagroup_description(self, group: str) -> DataGroupDescription:
        """Get the description of a datagroup."""

    @abstractmethod
    def get_dataset_description(self, group: str, dataset: str) -> DataSetDescription:
        """Get the description of a dataset."""

    @abstractmethod
    def get_columns_description(self, dataset: str) -> list[ColumnDescription]:
        """Get the description of columns in a dataset."""


class ClientProtocol(Protocol):
    """Adapter interface for DB clients."""

    @abstractmethod
    def compile(self, stmt: Select) -> CompiledQuery:
        """Compile a SQLAlchemy Select statement into a CompiledQuery."""

    @abstractmethod
    def get_dataset_columns(self, group: str, name: str) -> list[Column]:
        """Create a dataset metadata instance."""

    @abstractmethod
    def create_function_handle(self) -> FunctionHandle:
        """Create a FunctionHandle instance."""

    @abstractmethod
    def download(
        self,
        dataset: str,
        download_path: Path,
        date: Union[date_like, tuple[date_like, date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Union[date_like, tuple[date_like, date_like], None],
    ):
        """Download data from the dataset."""

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
