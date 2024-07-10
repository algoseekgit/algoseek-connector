"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""

import os
import warnings

from . import base, clickhouse, s3
from .dataset_api import DatasetAPIProvider
from .models import DataSourceType
from .settings import load_settings


class ResourceManager:
    """
    Manage data sources available to an user.

    Methods
    -------
    create_data_source:
        Create a new DataSource instance.
    list_data_source:
        List available data sources.

    """

    def __init__(self):
        for old, new in DEPRECATED_ENV_VARS_MAPPING.items():
            if old in os.environ and new not in os.environ:
                msg = (
                    f"{old} environment variable is deprecated, please use {new}. This will raise "
                    "an exception in a future release."
                )
                os.environ[new] = os.environ[old]
                warnings.warn(msg, DeprecationWarning, stacklevel=2)

        self._api = DatasetAPIProvider()

    def create_data_source(self, name: str, **kwargs) -> base.DataSource:
        """
        Create a connection to a data source.

        Parameters
        ----------
        name : str
            Name of an available data source.
        kwargs : dict
            Key-value parameters passed to the ClientProtocol used by the
            data source.

        Returns
        -------
        DataSource

        See Also
        --------
        :py:func:`~algoseek_connector.ResourceManager.list_data_sources`
            Provides a list text ids from available data sources.

        """
        if name in ["ardadb", "s3"]:
            deprecated = name
            name = DataSourceType.ARDADB.value if name == "ardadb" else DataSourceType.S3.value
            msg = (
                f"data source name `{deprecated}` is deprecated please use `{name}` or use the "
                "corresponding `DataSourceType` enumeration. This will raise an exception in a future "
                "release."
            )
            warnings.warn(msg, DeprecationWarning, stacklevel=2)

        if name not in self.list_data_sources():
            msg = f"{name} is not a valid data source."
            raise ValueError(msg)
        data_source_type = DataSourceType(name)
        client = self._create_client(data_source_type, **kwargs)
        description_provider = self._create_description_provider(data_source_type)
        return base.DataSource(client, description_provider)

    def _create_description_provider(self, type_: DataSourceType) -> base.DescriptionProvider:
        if type_ == DataSourceType.ARDADB:
            description_provider = clickhouse.ArdaDBDescriptionProvider(self._api)
        elif type_ == DataSourceType.S3:
            description_provider = s3.S3DescriptionProvider(self._api)
        else:
            raise NotImplementedError(DataSourceType)
        return description_provider

    def _create_client(self, type_: DataSourceType, **kwargs) -> base.ClientProtocol:
        settings = load_settings()
        if type_ == DataSourceType.ARDADB:
            ardadb_config = settings.ardadb.model_copy(update=kwargs)
            clickhouse_connect_client = clickhouse.create_clickhouse_client(ardadb_config)
            client = clickhouse.ClickHouseClient(clickhouse_connect_client)
        elif type_ == DataSourceType.S3:
            s3_config = settings.s3.model_copy(update=kwargs)
            secret_key = (
                None if s3_config.aws_secret_access_key is None else s3_config.aws_secret_access_key.get_secret_value()
            )
            session = s3.create_boto3_session(
                profile_name=s3_config.profile_name,
                aws_access_key_id=s3_config.aws_access_key_id,
                aws_secret_access_key=secret_key,
            )
            client = s3.S3DownloaderClient(session, self._api)
        else:
            raise NotImplementedError(type_)
        return client

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        return [x.value for x in DataSourceType]


DEPRECATED_ENV_VARS_MAPPING = {
    "ALGOSEEK_API_USERNAME": "ALGOSEEK__DATASET_API__EMAIL",
    "ALGOSEEK_API_PASSWORD": "ALGOSEEK__DATASET_API__PASSWORD",
    "ALGOSEEK_ARDADB_HOST": "ALGOSEEK__ARDADB__HOST",
    "ALGOSEEK_ARDADB_PORT": "ALGOSEEK__ARDADB__PORT",
    "ALGOSEEK_ARDADB_USERNAME": "ALGOSEEK__ARDADB__USER",
    "ALGOSEEK_ARDADB_PASSWORD": "ALGOSEEK__ARDADB__PASSWORD",
    "ALGOSEEK_AWS_PROFILE": "ALGOSEEK__S3__PROFILE_NAME",
    "ALGOSEEK_AWS_ACCESS_KEY_ID": "ALGOSEEK__S3__ACCESS_KEY_ID",
    "ALGOSEEK_AWS_SECRET_ACCESS_KEY": "ALGOSEEK__S3__SECRET_ACCESS_KEY",
}
