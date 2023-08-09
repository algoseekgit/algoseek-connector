"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""

from .base import ClientProtocol, DataSource
from .clickhouse.client import (
    ArdaDBDescriptionProvider,
    ClickHouseClient,
    create_clickhouse_client,
)
from .metadata_api import AuthToken, BaseAPIConsumer

ARDADB = "ardadb"


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
        self._client_factory = ClientProtocolFactory()
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
        client = self._client_factory(name, **kwargs)
        if name == ARDADB:
            description_provider = ArdaDBDescriptionProvider(self._api)
        else:
            raise ValueError

        return DataSource(client, description_provider)

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        sources = [ARDADB]
        return sources


class ClientProtocolFactory:
    """Create Client for data sources."""

    def __init__(self):
        self._clients = {ARDADB: ClickHouseClient}

    def __call__(self, client_type: str, **kwargs) -> ClientProtocol:
        """Create a ClientProtocol."""
        if client_type == ARDADB:
            ch_client = create_clickhouse_client(**kwargs)
            client = ClickHouseClient(ch_client)
        else:
            raise ValueError

        return client
