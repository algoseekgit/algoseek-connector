"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""

from . import base, clickhouse, s3
from .metadata_api import AuthToken, BaseAPIConsumer

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

    def create_data_source(self, name: str, **kwargs) -> base.DataSource:
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
        return base.DataSource(client, description_provider)

    def _create_description_provider(self, name: str) -> base.DescriptionProvider:
        if name == ARDADB:
            description_provider = clickhouse.ArdaDBDescriptionProvider(self._api)
        elif name == S3:
            description_provider = s3.S3DescriptionProvider(self._api)
        else:
            raise ValueError
        return description_provider

    def _create_client(self, name, **kwargs) -> base.ClientProtocol:
        if name == ARDADB:
            ch_client = clickhouse.create_clickhouse_client(**kwargs)
            client = clickhouse.ClickHouseClient(ch_client)
        elif name == S3:
            session = s3.create_boto3_session(**kwargs)
            client = s3.S3DownloaderClient(session, self._api)
        else:
            raise ValueError
        return client

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        sources = [ARDADB, S3]
        return sources
