"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""

from .base import ClientProtocol, DataSource, DescriptionProvider
from .clickhouse.client import (
    ArdaDBDescriptionProvider,
    ClickHouseClient,
    create_clickhouse_client,
)
from .metadata_api import AuthToken, BaseAPIConsumer
from .s3 import S3DescriptionProvider, S3DownloaderClient

ARDADB = "ardadb"
S3 = "s3"


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
        token = AuthToken()
        # TODO: add functionality to refresh token.
        self._api = BaseAPIConsumer(token)

    def create_data_source(self, name: str, **kwargs) -> DataSource:
        """
        Create a connection to a data source.

        Parameters
        ----------
        name : str
            Name of an available data source.

        Returns
        -------
        DataSource

        """
        client = self._create_client(name, **kwargs)
        description_provider = self._create_description_provider(name)
        return DataSource(client, description_provider)

    def _create_description_provider(self, name: str) -> DescriptionProvider:
        if name == ARDADB:
            description_provider = ArdaDBDescriptionProvider(self._api)
        elif name == S3:
            description_provider = S3DescriptionProvider(self._api)
        else:
            raise ValueError
        return description_provider

    def _create_client(self, name, **kwargs) -> ClientProtocol:
        if name == ARDADB:
            ch_client = create_clickhouse_client(**kwargs)
            client = ClickHouseClient(ch_client)
        elif name == S3:
            client = S3DownloaderClient(self._api)
        else:
            raise ValueError
        return client

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        sources = [ARDADB, S3]
        return sources
