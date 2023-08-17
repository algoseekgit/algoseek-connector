"""DataResource implementation for ClickHouse DB."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Generator, Optional, Union, cast

import clickhouse_connect
import sqlparse
from clickhouse_connect.driver import Client
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from pandas import DataFrame
from sqlalchemy import Column
from sqlalchemy.sql import Select

from .. import base, s3
from ..base import date_like
from ..metadata_api import BaseAPIConsumer
from .sqla_table import SQLAlchemyColumnFactory


class ClickHouseClient(base.ClientProtocol):
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

    def __init__(self, client: Client):
        self._client = client
        self._column_factory = SQLAlchemyColumnFactory()
        self._dialect = ClickHouseDialect(paramstyle="pyformat")

    def create_function_handle(self) -> base.FunctionHandle:
        """Get a FunctionHandler instance."""
        functions = ["sum", "average"]
        return base.FunctionHandle(functions)

    def execute(
        self,
        sql: str,
        parameters: Optional[dict] = None,
        output: str = "python",
        **kwargs,
    ):
        """
        Execute raw SQL queries.

        Parameters
        ----------
        sql : str
            Parametrized sql query.
        parameters : dict or None, default=None
            Query parameters.
        output : {"python", "dataframe"}
            Wether to output data using Python native types or Pandas DataFrames.
        kwargs :
            Optional parameters passed to clickhouse-connect Client.query
            method.

        Returns
        -------
        dict or pandas.DataFrame
            If `size` is ``None``.

        Yields
        ------
        dict or pandas.DataFrame
            If `size` is specified.

        """
        if parameters is None:
            parameters = dict()
        query = base.CompiledQuery(sql, parameters)

        if output == "python":
            return self.fetch(query, **kwargs)
        elif output == "dataframe":
            return self.fetch_dataframe(query, **kwargs)
        else:
            msg = f"Valid outputs are either `python` or `dataframe`. Got {output}."
            raise ValueError(msg)

    def download(
        self,
        dataset: str,
        download_path: Path,
        date: Union[date_like, tuple[date_like, date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Union[date_like, tuple[date_like, date_like]],
    ):  # pragma: no cover
        """Not implemented."""
        raise NotImplementedError

    def fetch(self, query: base.CompiledQuery, **kwargs) -> dict[str, tuple]:
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
        self, query: base.CompiledQuery, size: int, **kwargs
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

    def fetch_dataframe(self, query: base.CompiledQuery, **kwargs) -> DataFrame:
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
        self, query: base.CompiledQuery, size: int, **kwargs
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
    def get_dataset_columns(self, group: str, dataset: str) -> list[Column]:
        """
        Create SQLAlchemy columns for the dataset.

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
            col_description = base.ColumnDescription(col_name, t, description)
            column = self._column_factory(col_description)
            columns.append(column)
        return columns

    def compile(self, stmt: Select, **kwargs) -> base.CompiledQuery:
        """Convert a stmt into an SQL string."""
        compile_kwargs = {"compile_kwargs": {"render_postcompile": True}}
        compile_kwargs.update(kwargs)
        compiled = stmt.compile(dialect=self._dialect, **compile_kwargs)
        sql_format_params = {
            "reindent": True,
            "indent_width": 4,
        }
        compiled_string = sqlparse.format(compiled.string, **sql_format_params)
        return base.CompiledQuery(compiled_string, compiled.params)

    def store_to_s3(
        self,
        query: base.CompiledQuery,
        bucket: str,
        key: str,
        profile_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        **kwargs,
    ):
        """
        Execute a query and store results into an S3 object.

        Parameters
        ----------
        query : CompiledQuery
        bucket : str
            The bucket name used to store the query.
        key : str
            The name of the object where the query is going to be stored.
        profile_name : str or None, default=None
            A profile name defined in `~/.aws/credentials`. If a profile name is
            specified, the access key and secret key are retrieved from this
            file and the parameters `aws_access_key_id` and
            `aws_secret_access_key` are ignored. If ``None``, this field is
            ignored.
        aws_access_key_id : str or None, default=None
            The AWS access key associated with an IAM user or role. If ``None``,
            the key is retrieved from the  `AWS_ACCESS_KEY_ID` environment
            variable.
        aws_secret_access_key : str or None, default=None
            Thee secret key associated with the access key. If ``None``, the key
            is retrieved from the  `AWS_ACCESS_KEY_ID` environment variable.
        kwargs
            Key-value arguments passed to clickhouse-connect Client.query
            method.

        Raises
        ------
        ValueError
            If a non-existing bucket name is passed or if trying to overwrite
            an existing object.

        """
        # check access to bucket and if object does not exist
        boto3_session = s3.create_boto3_session(
            profile_name, aws_access_key_id, aws_secret_access_key
        )
        s3_client = s3.downloader.get_s3_client(boto3_session)
        bucket_obj = s3.downloader.BucketWrapper(s3_client, bucket)
        if bucket_obj.check_object_exists(key):
            msg = f"Object with key={key} already exists in bucket {bucket}."
            raise ValueError(msg)
        url = bucket_obj.get_object_url(key)
        credentials = boto3_session.get_credentials()
        aws_key_id = cast(str, credentials.access_key)
        aws_secret_access_key = cast(str, credentials.secret_key)

        sql = _create_insert_to_s3_query(
            query.sql, url, aws_key_id, aws_secret_access_key
        )
        self._client.query(sql, query.parameters, **kwargs)


class ArdaDBDescriptionProvider(base.DescriptionProvider):
    """Provide descriptions for ArdaDB datasets."""

    def __init__(self, api: BaseAPIConsumer) -> None:
        self._api = api

    @lru_cache
    def _ardadb_group_to_api_group(self) -> dict[str, str]:
        """Create a dictionary that maps the group name used in ArdaDB to the API name."""
        api_groups = self._api.list_datagroups()
        res = dict()
        for group in api_groups:
            group_metadata = self._api.get_datagroup_metadata(group)
            full_name = group_metadata["full_name"]
            ardadb_group = full_name.replace(" ", "")
            res[ardadb_group] = group
        return res

    @lru_cache
    def _ardadb_dataset_to_api_dataset(self) -> dict[str, str]:
        """Create a dictionary that maps the dataset name used in ArdaDB to the API name."""
        api_datasets = self._api.list_datasets()
        res = dict()
        for dataset in api_datasets:
            dataset_metadata = self._api.get_dataset_metadata(dataset)
            db_metadata = dataset_metadata.get("database_table")
            if db_metadata is not None:
                # table name is DBName.TableName
                ardadb_dataset = db_metadata["table_name"].split(".")[-1]
                res[ardadb_dataset] = dataset
        return res

    def get_datagroup_description(self, group: str) -> base.DataGroupDescription:
        """
        Get the description of a datagroup.

        Parameters
        ----------
        group : str
            The data group name.

        Returns
        -------
        DataGroupDescription

        """
        try:
            group_text_id = self._ardadb_group_to_api_group()[group]
            datagroup_metadata = self._api.get_datagroup_metadata(group_text_id)
            display_name = datagroup_metadata["display_name"]
            description = datagroup_metadata["description"]
        except KeyError:
            description = ""
            display_name = group
        return base.DataGroupDescription(group, description, display_name)

    def get_columns_description(self, dataset: str) -> list[base.ColumnDescription]:
        """
        Get the description of the dataset columns.

        Parameters
        ----------
        dataset : str
            The dataset name.

        Returns
        -------
        list[ColumnDescription]

        """
        try:
            dataset_text_id = self._ardadb_dataset_to_api_dataset()[dataset]
            dataset_metadata = self._api.get_dataset_metadata(dataset_text_id)
            db_metadata = dataset_metadata["database_table"]
            columns = list()
            for column in db_metadata["sql_columns"]:
                c = base.ColumnDescription(
                    column["name"], column["data_type_db"], column["description"]
                )
                columns.append(c)
        except KeyError:
            columns = list()
        return columns

    def get_dataset_description(
        self, group: str, dataset: str
    ) -> base.DataSetDescription:
        """
        Get the description of a dataset.

        group : str
            The datagroup name.
        dataset : str
            The dataset name.

        Returns
        -------
        DatasetDescription

        """
        columns = self.get_columns_description(dataset)
        try:
            dataset_text_id = self._ardadb_dataset_to_api_dataset()[dataset]
            dataset_metadata = self._api.get_dataset_metadata(dataset_text_id)
            display_name = dataset_metadata["display_name"]
            description = dataset_metadata["long_description"]
        except KeyError:
            display_name = dataset
            description = ""
        return base.DataSetDescription(
            dataset, group, columns, display_name, description
        )


def create_clickhouse_client(
    host: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    **kwargs,
) -> Client:
    """Create a ClickHouse DB client."""
    default_port = 8123
    host = host or os.getenv("ALGOSEEK_DATABASE_HOST")
    if port is None:
        port_env = os.getenv("ALGOSEEK_DATABASE_PORT")
        port = default_port if port_env is None else int(port_env)
    user = user or os.getenv("ALGOSEEK_DATABASE_USER")
    password = password or os.getenv("ALGOSEEK_DATABASE_PASSWORD")
    return clickhouse_connect.get_client(
        host=host, port=port, user=user, password=password, **kwargs
    )


def _create_insert_to_s3_query(
    sql: str, url: str, aws_key_id: str, aws_secret_access_key: str
) -> str:
    s3_call = f"s3('{url}', '{aws_key_id}', '{aws_secret_access_key}', CSVWithNames)"
    return f"INSERT INTO FUNCTION {s3_call}\n {sql}"
