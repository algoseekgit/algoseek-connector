"""DataResource implementation for ClickHouse DB."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Generator, Optional, cast

import clickhouse_connect
from clickhouse_connect.driver import Client
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from pandas import DataFrame
from sqlalchemy.sql import Select

from ..base import ClientProtocol, CompiledQuery, DataSetMetadata, FunctionHandle
from .base import ColumnMetadata
from .sqla_table import SQLAlchemyColumnFactory


class ClickHouseClient(ClientProtocol):
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
    **kwargs : dict
        Optional arguments passed to clickhouse_connect.get_client.

    """

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ):
        self._client = _create_clickhouse_client(host, user, password, port, **kwargs)
        self._column_factory = SQLAlchemyColumnFactory()
        self._dialect = ClickHouseDialect(paramstyle="pyformat")

    def create_function_handle(self) -> FunctionHandle:
        """Get a FunctionHandler instance."""
        functions = ["sum", "average"]
        return FunctionHandle(functions)

    def fetch(self, query: CompiledQuery, **kwargs) -> dict[str, tuple]:
        """
        Retrieve data using a select statement.

        Parameters
        ----------
        stmt : Select
            A select statement generated with :py:meth:`Dataset.select`.
        kwargs :
            Optional parameters passed to clickhouse-connect Client.query
            method.

        Returns
        -------
        dict[str, tuple]
            A mapping from column names to values retrieved.

        """
        query_result = self._client.query(query.sql, query.parameters, **kwargs)
        names = query_result.column_names
        data = query_result.result_columns
        result = dict()
        for column, name in zip(data, names):
            result[name] = column
        return result

    def fetch_iter(
        self, query: CompiledQuery, size: int, **kwargs
    ) -> Generator[dict[str, tuple], None, None]:
        """
        Retrieve data with result streaming using a select statement.

        Parameters
        ----------
        stmt : Select
            A select statement generated with :py:meth:`Dataset.select`.
        size : int
            Sets the `max_block_size_parameter` of the ClickHouse DataBase.
            Values lower than ``8912`` are ignored. Overwrites values passed
            using settings as optional parameter
        kwargs :
            Optional parameters passed to clickhouse-connect
            Client.query_column_block_stream method.

        Yields
        ------
        dict[str, tuple]
            A mapping from column names to values retrieved.

        """
        settings = {"max_block_size": size}
        kwargs_settings = kwargs.get("settings", dict())
        kwargs_settings.update(settings)

        with self._client.query_column_block_stream(
            query.sql, parameters=query.parameters, **kwargs
        ) as stream:
            column_names = stream.source.column_names
            for block in stream:
                yield {k: v for k, v in zip(column_names, block)}

    def fetch_dataframe(self, query: CompiledQuery, **kwargs) -> DataFrame:
        """
        Execute a Select statement and output data as a Pandas DataFrame.

        Parameters
        ----------
        query : CompiledQuery
        kwargs :
            Optional parameters passed to clickhouse-connect
            Client.query_df method.

        Returns
        -------
        pandas.DataFrame

        """
        return self._client.query_df(query.sql, query.parameters, **kwargs)

    def fetch_iter_dataframe(
        self, query: CompiledQuery, size: int, **kwargs
    ) -> Generator[DataFrame, None, None]:
        """
        Yield pandas DataFrame in chunks.

        Parameters
        ----------
        stmt : Select
            A select statement generated with :py:meth:`Dataset.select`.
        size : int
            Sets the `max_block_size_parameter` of the ClickHouse DataBase.
            Values lower than ``8912`` are ignored. Overwrites values passed
            using settings as optional parameter
        kwargs :
            Optional parameters passed to clickhouse-connect
            Client.query_df_stream method.

        Yields
        ------
        pandas.DataFrame

        """
        settings = {"max_block_size": size}
        kwargs_settings = kwargs.get("settings", dict())
        kwargs_settings.update(settings)
        with self._client.query_df_stream(
            query.sql, parameters=query.parameters, settings=settings
        ) as stream:
            for df in stream:
                yield df

    @lru_cache
    def list_datagroups(self) -> list[str]:
        """List available groups."""
        sql = "SHOW DATABASES"
        group_names = self._client.query(sql).result_columns[0]
        return list(group_names)

    @lru_cache
    def list_datasets(self, group: str) -> list[str]:
        """List available datasets in the data group."""
        sql = f"SHOW TABLES FROM {group}"
        table_names = self._client.query(sql).result_columns
        return list(table_names[0]) if table_names else list()

    @lru_cache
    def fetch_dataset_metadata(self, group: str, dataset: str) -> DataSetMetadata:
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
        DatasetMetadata

        Raises
        ------
        ValueError
            If an invalid data group or dataset name are provided.

        """
        sql = f"DESCRIBE TABLE {group}.{dataset}"
        query = self._client.query(sql).result_columns
        col_names, col_types, _, _, col_descriptions, _, _ = query
        columns = list()
        for col_name, t, description in zip(col_names, col_types, col_descriptions):
            column_metadata = ColumnMetadata(col_name, t, description)
            column = self._column_factory(column_metadata)
            columns.append(column)
        return DataSetMetadata(dataset, columns)

    def compile(self, stmt: Select, **kwargs) -> CompiledQuery:
        """Convert a stmt into an SQL string."""
        compile_kwargs = {"compile_kwargs": {"render_postcompile": True}}
        compile_kwargs.update(kwargs)
        compiled = stmt.compile(dialect=self._dialect, **compile_kwargs)
        return CompiledQuery(compiled.string, compiled.params)


def _create_clickhouse_client(
    host: Optional[str],
    user: Optional[str],
    password: Optional[str],
    port: Optional[int],
    **kwargs,
) -> Client:
    """Create a ClickHouse DB client."""
    # TODO: fix hardcoded port value
    default_port = 9000
    host = host or os.getenv("ALGOSEEK_DATABASE_HOST")
    if port is None:
        port_env = os.getenv("ALGOSEEK_DATABASE_PORT")
        port = default_port if port_env is None else int(port_env)
    port = 8123
    user = user or os.getenv("ALGOSEEK_DATABASE_USER")
    password = password or os.getenv("ALGOSEEK_DATABASE_PASSWORD")
    return clickhouse_connect.get_client(
        host=host, port=port, user=user, password=password, **kwargs
    )


class MockClickHouseClient(ClickHouseClient):
    """Mock class used for testing query string generation."""

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
        self._client = cast(Client, None)
        self._column_factory = SQLAlchemyColumnFactory()
        self._dialect = ClickHouseDialect(paramstyle="pyformat")

    def list_datagroups(self) -> list[str]:
        """Overwrite call to DB in method."""
        return list()

    def list_datasets(self, group: str) -> list[str]:
        """Overwrite call to DB in method."""
        return list()
