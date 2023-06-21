"""DataResource implementation for ClickHouse DB."""

import numpy as np
import os
import sqlalchemy
from typing import cast, Optional
from clickhouse_driver import Client
from sqlalchemy import Select
from ..base import DataGroup, DataResource, DataSet, FunctionHandle
from .base import ColumnMetadata, TableMetadata
from .sqla_table import ClickHouseDialect, SQLAlchemyTableFactory
from functools import lru_cache
from pandas import DataFrame


class ClickHouseDataResource(DataResource):
    """Manage dataset retrieval from ClickHouse DB."""

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        secure: bool = False,
        **kwargs,
    ):
        self._client = _create_clickhouse_client(
            host, user, password, port, secure, **kwargs
        )
        self._table_metadata_factory = ClickHouseTableMetadataFactory(self._client)
        self._table_factory = SQLAlchemyTableFactory()
        self.groups: dict[str, DataGroup] = dict()
        self._dialect = ClickHouseDialect(paramstyle="pyformat")

    def get_function_handle(self) -> FunctionHandle:
        """Get a FunctionHandler instance."""
        functions = ["sum", "average"]
        return FunctionHandle(functions)

    def fetch(self, stmt: sqlalchemy.Select, **kwargs):
        """Execute a Select statement."""
        query = stmt.compile(dialect=self._dialect)
        execute_params = {"with_column_types": True, "columnar": True}
        if "settings" in kwargs:
            execute_params["settings"] = kwargs["settings"]

        data, names = cast(
            tuple[list[tuple], list[tuple]],
            self._client.execute(query.string, query.params, **execute_params),
        )
        result = dict()
        for column, (name, _) in zip(data, names):
            result[name] = column
        return result

    def fetch_dataframe(self, stmt: Select) -> DataFrame:
        """Execute a Select statement and output data as a Pandas DataFrame."""
        return DataFrame(self.fetch_numpy(stmt))

    def fetch_numpy(self, stmt: Select) -> dict[str, np.ndarray]:
        """Execute a Select statement and output data as a Pandas DataFrame."""
        query_kwargs = {"settings": {"use_numpy": True}}
        return self.fetch(stmt, **query_kwargs)

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
            data_group.add_dataset(dataset)
        return dataset

    def _create_dataset(self, group: str, name: str) -> DataSet:
        """Create a new dataset."""
        data_group = self.get_datagroup(group)
        table_metadata = self._table_metadata_factory(group, name)
        table = self._table_factory(table_metadata, data_group.metadata)
        return DataSet(data_group, table, self)


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
