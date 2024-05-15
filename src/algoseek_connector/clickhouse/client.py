"""DataResource implementation for ClickHouse DB."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Generator, Optional, Union, cast

import clickhouse_connect
import sqlparse
from clickhouse_connect.driver.client import Client
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from pandas import DataFrame
from sqlalchemy import Column
from sqlalchemy.sql import Select

from .. import base, s3
from ..base import date_like
from ..dataset_api import DatasetAPIProvider
from ..models import ArdaDBConfiguration
from .sqla_table import SQLAlchemyColumnFactory


class ClickHouseClient(base.ClientProtocol):
    """
    Manage dataset retrieval from ClickHouse DB.

    Parameters
    ----------
    client : clickhouse_connect.Client

    Methods
    -------
    create_function_handle:
        Create a FunctionHandle instance.
    execute:
        Execute raw SQL queries.
    download:
        Not Implemented.
    fetch:
        Retrieve data in Python native format using
        :py:class:`sqlalchemy.sql.selectable.Select`.
    fetch_iter:
        Retrieve data in Python native format using
        :py:class:`sqlalchemy.sql.selectable.Select`. Stream results.
    fetch_dataframe:
        Retrieve data as a Pandas DataFrame using
        :py:class:`sqlalchemy.sql.selectable.Select`.
    fetch_iter_dataframe:
        Retrieve data as a Pandas DataFrame using
        :py:class:`sqlalchemy.sql.selectable.Select`. Stream results.
    list_datagroups:
        List available data groups.
    list_datasets:
        List available datasets.
    get_dataset_columns:
        Create a list of :py:class:`sqlalchemy.Column` for a dataset.
    compile:
        Converts a :py:class:`sqlalchemy.sql.selectable.Select` into a
        :py:class:`~algoseek_connector.base.CompiledQuery.`
    Store_to_s3:
        Store query results into a S3 object.

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
            Wether to output data using a dictionary or a Pandas DataFrame.
        kwargs :
            Optional parameters passed to clickhouse-connect Client.query
            method.

        Returns
        -------
        dict or pandas.DataFrame

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
        query : CompiledQuery
            The query statement to fetch.
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

    def fetch_iter(self, query: base.CompiledQuery, size: int, **kwargs) -> Generator[dict[str, tuple], None, None]:
        """
        Retrieve data with result streaming using a select statement.

        Parameters
        ----------
        query : CompiledQuery
            The query statement to fetch.
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

        with self._client.query_column_block_stream(query.sql, parameters=query.parameters, **kwargs) as stream:
            column_names = stream.source.column_names
            for block in stream:
                yield {k: v for k, v in zip(column_names, block)}

    def fetch_dataframe(self, query: base.CompiledQuery, **kwargs) -> DataFrame:
        """
        Execute a Select statement and output data as a Pandas DataFrame.

        Parameters
        ----------
        query : CompiledQuery
            The query statement to fetch.
        kwargs :
            Optional parameters passed to clickhouse-connect
            Client.query_df method.

        Returns
        -------
        pandas.DataFrame

        """
        return self._client.query_df(query.sql, query.parameters, **kwargs)

    def fetch_iter_dataframe(self, query: base.CompiledQuery, size: int, **kwargs) -> Generator[DataFrame, None, None]:
        """
        Yield pandas DataFrame in chunks.

        Parameters
        ----------
        query : CompiledQuery
            The query statement to fetch.
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
        with self._client.query_df_stream(query.sql, parameters=query.parameters, settings=settings) as stream:
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
        dataset : str
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
            The query statement to fetch.
        bucket : str
            The bucket name used to store the query.
        key : str
            The name of the object where the query is going to be stored.
        profile_name : str or None, default=None
            If a profile name is specified, the access key and secret key are
            retrieved from  `~/.aws/credentials` and the parameters
            `aws_access_key_id` and `aws_secret_access_key` are ignored. If
            ``None``, this field is ignored.
        aws_access_key_id : str or None, default=None
            The AWS access key associated with an IAM user or role.
        aws_secret_access_key : str or None, default=None
            Thee secret key associated with the access key.
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
        boto3_session = s3.create_boto3_session(profile_name, aws_access_key_id, aws_secret_access_key)
        s3_client = s3.downloader.get_s3_client(boto3_session)
        bucket_obj = s3.downloader.BucketWrapper(s3_client, bucket)
        if bucket_obj.check_object_exists(key):
            msg = f"Object with key={key} already exists in bucket {bucket}."
            raise ValueError(msg)
        url = bucket_obj.get_object_url(key)
        credentials = boto3_session.get_credentials()
        aws_key_id = cast(str, credentials.access_key)
        aws_secret_access_key = cast(str, credentials.secret_key)

        sql = _create_insert_to_s3_query(query.sql, url, aws_key_id, aws_secret_access_key)
        self._client.query(sql, query.parameters, **kwargs)


class ArdaDBDescriptionProvider(base.DescriptionProvider):
    """Provide descriptions for ArdaDB datasets."""

    def __init__(self, api: DatasetAPIProvider) -> None:
        self.api = api

    @lru_cache
    def _ardadb_schema_to_api_group(self) -> dict[str, str]:
        """Create a dictionary that maps the group name used in ArdaDB to the API name."""
        res = dict()
        for destination_id in self.api.list_dataset_destinations():
            dataset = self.api.get_dataset(destination_id)
            if dataset.destination_type == "ArdaDB":
                res[dataset.schema_name] = dataset.data_group_name
        return res

    @lru_cache
    def _ardadb_table_to_api_dataset(self) -> dict[str, dict[str, int]]:
        """Create a dictionary that maps the dataset name used in ArdaDB to the API name."""
        res = dict()
        for destination_id in self.api.list_dataset_destinations():
            dataset = self.api.get_dataset(destination_id)
            if dataset.destination_type == "ArdaDB" and dataset.table_name is not None:
                schema_name, table_name = dataset.table_name.split(".")
                group_dict = res.setdefault(schema_name, dict())
                group_dict[table_name] = dataset.destination_id
        return res

    def _get_api_data_group_name(self, ardadb_group: str) -> str:
        return self._ardadb_schema_to_api_group()[ardadb_group]

    def _get_api_dataset_destination_id(self, ardadb_group: str, ardadb_dataset: str) -> int:
        return self._ardadb_table_to_api_dataset()[ardadb_group][ardadb_dataset]

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
            data_group_name = self._get_api_data_group_name(group)
            info = self.api.get_data_group(data_group_name)
            display_name = info.display_name
            description = info.description
        except KeyError:
            description = ""
            display_name = group
        return base.DataGroupDescription(group, description, display_name)

    def get_columns_description(self, group: str, dataset: str) -> list[base.ColumnDescription]:
        """
        Get the description of the dataset columns.

        Parameters
        ----------
        group : str
            The data group name.
        dataset : str
            The dataset name.

        Returns
        -------
        list[ColumnDescription]

        """
        try:
            destination_id = self._get_api_dataset_destination_id(group, dataset)
            info = self.api.get_dataset_details(destination_id)
            columns = list()
            for column in info.data_columns:
                description = "" if column.description is None else column.description
                c = base.ColumnDescription(column.name, column.data_type, description)
                columns.append(c)
        except KeyError:
            columns = list()
        return columns

    def get_dataset_description(self, group: str, dataset: str) -> base.DataSetDescription:
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
        try:
            destination_id = self._get_api_dataset_destination_id(group, dataset)
        except KeyError:
            # create empty description when requesting a dataset not available in the API
            return base.DataSetDescription(dataset, group, list())

        info = self.api.get_dataset(destination_id)
        columns = self.get_columns_description(group, dataset)
        long_description = self.api.get_dataset_details(destination_id).long_description
        if long_description is None:
            long_description = info.short_description

        return base.DataSetDescription(
            dataset,
            group,
            columns,
            info.dataset_display_name,
            long_description,
            info.time_granularity,
            info.documentation_link,
            info.sample_data_url,
        )


def create_clickhouse_client(config: ArdaDBConfiguration) -> Client:
    """
    Create a clickhouse_connect.Client instance.

    Default values are obtained from the user configuration. See here
    TODO: add link for a guide on how to set user configuration.

    Parameters
    ----------
    config: ArdaDBConfiguration
        the ArdaDB data source configuration.

    """
    return clickhouse_connect.get_client(
        host=config.host, port=config.port, user=config.user, password=config.password, **config.extra
    )


def _create_insert_to_s3_query(sql: str, url: str, aws_key_id: str, aws_secret_access_key: str) -> str:
    s3_call = f"s3('{url}', '{aws_key_id}', '{aws_secret_access_key}', CSVWithNames)"
    return f"INSERT INTO FUNCTION {s3_call}\n {sql}"
