"""DataResource implementation for ClickHouse DB."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Generator, Optional, cast

from clickhouse_driver import Client
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from sqlalchemy.sql import Select

from ..base import CompiledQuery, DataGroup, DataResource, DataSet, FunctionHandle
from .base import ColumnMetadata, TableMetadata
from .sqla_table import ClickHouseToNumpyTypeMapper, SQLAlchemyTableFactory

if TYPE_CHECKING:  # pragma: no cover
    import numpy as np
    from pandas import DataFrame


class ClickHouseDataResource(DataResource):
    """
    Manage dataset retrieval from ClickHouse DB.

    Parameters
    ----------
    host : str or None, default=None
        Host address running a ClickHouse server. If ``None``, the address is
        set through the environment variable `ALGOSEEK_DATABASE_HOST`.
    port : int or None, default=None
        port ClickHouse server is bound to. If ``None``, the port is set
        through the environment variable `ALGOSEEK_DATABASE_PORT`.
    user : str or None, default=None
        Database user. If ``None``, the user is set through the environment
        variable `ALGOSEEK_DATABASE_USER`.
    password : str or None, default=None
        User's password. If ``None``, the password is set through the
        environment variable `ALGOSEEK_DATABASE_PASSWORD`.
    use_numpy : bool, default=False
        If ``True``, retrieve data using numpy types. Otherwise, use Python
        native types. Numpy type support is limited to `Float32/64`,
        `[U]Int8/16/32/64`, `Date/DateTime(timezone)/DateTime64(timezone)`,
        `String/FixedString(N)`, `LowCardinality(T)` and `Nullable(T)`.
    **kwargs : dict
        Optional arguments passed to clickhouse_driver.Client constructor.
        The Client parameters `columnar` and `with_column_types` are always set
        to ``True``.

    """

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        secure: bool = False,
        use_numpy: bool = False,
        **kwargs,
    ):
        settings = kwargs.setdefault("settings", dict())
        settings["use_numpy"] = use_numpy

        self._client = _create_clickhouse_client(
            host, user, password, port, secure, **kwargs
        )
        self._table_metadata_factory = ClickHouseTableMetadataFactory(self._client)
        self._table_factory = SQLAlchemyTableFactory()
        self._numpy_type_mapper = ClickHouseToNumpyTypeMapper()
        self.groups: dict[str, DataGroup] = dict()
        self._dialect = ClickHouseDialect(paramstyle="pyformat")

    def get_function_handle(self) -> FunctionHandle:
        """Get a FunctionHandler instance."""
        functions = ["sum", "average"]
        return FunctionHandle(functions)

    def fetch(self, stmt: Select) -> dict[str, Any]:
        """
        Retrieve data using a select statement.

        Parameters
        ----------
        stmt : Select
            A select statement generated with :py:meth:`Dataset.select`.

        Returns
        -------
        dict[str, tuple]
            A mapping from column names to values retrieved.

        """
        query = stmt.compile(dialect=self._dialect)
        execute_params = {"with_column_types": True, "columnar": True}
        data, names = cast(
            tuple[list, list[tuple[str, str]]],
            self._client.execute(query.string, query.params, **execute_params),
        )

        result = dict()
        for column, (name, _) in zip(data, names):
            result[name] = column
        return result

    def fetch_iter(
        self, stmt: Select, size: int
    ) -> Generator[dict[str, Any], None, None]:
        """
        Retrieve data with result streaming using a select statement.

        Parameters
        ----------
        stmt : Select
            A select statement generated with :py:meth:`Dataset.select`.

        Yields
        ------
        dict[str, tuple]
            A mapping from column names to values retrieved.

        """
        query = stmt.compile(dialect=self._dialect)
        execute_params = {"with_column_types": True}
        iterator = self._client.execute_iter(
            query.string, query.params, chunk_size=size, **execute_params
        )

        column_names = None
        for chunk in iterator:
            # converts the chunk from a list of rows into a list of columns
            column_chunk = list(zip(*chunk))

            if column_names is None:
                # extract column names from the first row of data
                column_names = [x[0][0] for x in column_chunk]
                d = {col[0][0]: col[1:] for col in column_chunk}
            else:
                d = {k: v for k, v in zip(column_names, column_chunk)}

            yield d

    def fetch_dataframe(self, stmt: Select) -> DataFrame:
        """Execute a Select statement and output data as a Pandas DataFrame."""
        from pandas import DataFrame

        return DataFrame(self.fetch_numpy(stmt))

    def fetch_numpy(self, stmt: Select) -> dict[str, np.ndarray]:
        """Execute a Select statement and output data as a Pandas DataFrame."""
        from numpy import array

        return {k: array(v) for k, v in self.fetch(stmt).items()}

    def list_groups(self) -> list[str]:
        """List available groups."""
        return self._table_metadata_factory.list_groups()

    def list_datasets(self, group: str) -> list[str]:
        """List available datasets in the data group."""
        return self._table_metadata_factory.list_tables(group)

    def get_datagroup(self, group: str) -> DataGroup:
        """
        Retrieve a datagroup.

        Parameters
        ----------
        group : str
            The data group name.

        Returns
        -------
        DataGroup

        Raises
        ------
        ValueError
            If an invalid group name is provided.

        """
        valid_groups = self.list_groups()
        if group not in valid_groups:
            msg = f"{group} is not a valid data group name. Valid groups are one of {valid_groups}"
            raise ValueError(msg)
        return self.groups.setdefault(group, DataGroup(self))

    def get_dataset(self, group: str, name: str) -> DataSet:
        """
        Retrieve a dataset.

        Parameters
        ----------
        group : str
            Data group name.
        name : str
            Dataset name.

        Returns
        -------
        Dataset

        Raises
        ------
        ValueError
            If an invalid data group or dataset name are provided.

        """
        data_group = self.get_datagroup(group)
        dataset = data_group.get_dataset(name)
        if dataset is None:
            dataset = self._create_dataset(group, name)
        return dataset

    def _create_dataset(self, group: str, name: str) -> DataSet:
        """Create a new dataset."""
        data_group = self.get_datagroup(group)
        table_metadata = self._table_metadata_factory(group, name)
        table = self._table_factory(table_metadata, data_group.metadata)
        return DataSet(data_group, table, self)

    def compile(self, stmt: Select, **kwargs) -> CompiledQuery:
        """Convert a stmt into an SQL string."""
        compiled = stmt.compile(dialect=self._dialect, **kwargs)
        return CompiledQuery(compiled.string, compiled.params)


class ClickHouseTableMetadataFactory:
    """Get metadata from ClickHouse DB using SQL queries."""

    def __init__(self, client: Client) -> None:
        self._client = client

    @lru_cache
    def list_groups(self) -> list[str]:
        """List available Databases."""
        sql = "SHOW DATABASES"
        result = cast(list[list[str]], self._client.execute(sql, columnar=True))
        return list(result[0])

    @lru_cache
    def list_tables(self, group: str) -> list[str]:
        """List available tables."""
        self._validate_group(group)
        sql = f"SHOW TABLES FROM {group}"
        result = cast(list[list[str]], self._client.execute(sql, columnar=True))
        return list(result[0]) if result else list()

    @lru_cache
    def __call__(self, group: str, table: str) -> TableMetadata:
        """Create a table metadata instance."""
        self._validate_table(group, table)
        sql = f"DESCRIBE TABLE {group}.{table}"
        query_result = cast(list[list[str]], self._client.execute(sql, columnar=True))
        col_names, col_types, _, _, col_descriptions, _, _ = query_result
        columns = list()
        for name, t, description in zip(col_names, col_types, col_descriptions):
            col = ColumnMetadata(name, t, description)
            columns.append(col)
        return TableMetadata(table, group, columns)

    def _validate_group(self, group: str):
        valid_groups = self.list_groups()
        if group not in valid_groups:
            msg = f"{group} is not a valid group. Valid groups are {valid_groups}."
            raise ValueError(msg)

    def _validate_table(self, group: str, table: str):
        valid_tables = self.list_tables(group)
        if table not in valid_tables:
            msg = f"{table} is not a valid table in DB {group}. Valid tables are {valid_tables}."
            raise ValueError(msg)


def _create_clickhouse_client(
    host: Optional[str],
    user: Optional[str],
    password: Optional[str],
    port: Optional[int],
    secure: bool,
    **kwargs,
) -> Client:
    """Create a ClickHouse DB client."""
    default_port = 9000
    host = host or os.getenv("ALGOSEEK_DATABASE_HOST")
    if port is None:
        port_env = os.getenv("ALGOSEEK_DATABASE_PORT")
        port = default_port if port_env is None else int(port_env)
    user = user or os.getenv("ALGOSEEK_DATABASE_USER")
    password = password or os.getenv("ALGOSEEK_DATABASE_PASSWORD")
    return Client(host=host, user=user, password=password, secure=secure, **kwargs)


class MockClickHouseDataResource(ClickHouseDataResource):
    """Mock class use for testing query string generation."""

    def __init__(self):
        self.groups: dict[str, DataGroup] = dict()
        self._dialect = ClickHouseDialect(paramstyle="pyformat")

    def list_groups(self) -> list[str]:
        """List available groups."""
        return list(self.groups)
