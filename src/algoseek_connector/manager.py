"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""
from typing import cast

from . import base, clickhouse, config
from . import constants as c
from . import s3
from .metadata_api import AuthToken, BaseAPIConsumer


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
        metadata_services_settings = config.Settings().get_group(
            c.METADATA_SERVICE_SETTINGS_GROUP
        )
        api_credentials = metadata_services_settings.get_dict()
        token = AuthToken(**api_credentials)
        self._api = BaseAPIConsumer(token)

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
        if name not in self.list_data_sources():
            msg = f"{name} is not a valid data source."
            raise ValueError(msg)
        client = self._create_client(name, **kwargs)
        description_provider = self._create_description_provider(name)
        return base.DataSource(client, description_provider)

    def _create_description_provider(self, name: str) -> base.DescriptionProvider:
        if name == c.ARDADB:
            description_provider = clickhouse.ArdaDBDescriptionProvider(self._api)
        else:  # S3
            description_provider = s3.S3DescriptionProvider(self._api)
        return description_provider

    def _create_client(self, name, **kwargs) -> base.ClientProtocol:
        if name == c.ARDADB:
            ardadb_config = config.Settings().get_group(c.ARDADB).get_dict()
            credentials = cast(dict, ardadb_config.pop(c.CREDENTIAL_GROUP))
            user_settings = cast(dict, ardadb_config.pop(c.SETTINGS_GROUP))
            user_settings.update(credentials)
            user_settings.update(kwargs)
            clickhouse_client = clickhouse.create_clickhouse_client(**user_settings)
            client = clickhouse.ClickHouseClient(clickhouse_client)
        else:  # S3
            user_config = config.Settings()
            s3_config = user_config.get_group(c.S3).get_dict()
            session_credentials = cast(dict, s3_config.pop(c.CREDENTIAL_GROUP))
            settings = cast(dict, s3_config.pop(c.SETTINGS_GROUP))
            settings.update(session_credentials)
            settings.update(kwargs)
            session = s3.create_boto3_session(**settings)
            client = s3.S3DownloaderClient(session, self._api)
        return client

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        return c.DATA_SOURCES
