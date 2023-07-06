"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""

from .base import ClientProtocol, DataSource
from .clickhouse.client import ClickHouseClient


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
        return DataSource(client)

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        sources = ["clickhouse"]
        return sources


class ClientProtocolFactory:
    """Create Client for data sources."""

    def __init__(self):
        self._clients = {"clickhouse": ClickHouseClient}

    def __call__(self, client_type: str, **kwargs) -> ClientProtocol:
        """Create a ClientProtocol."""
        return self._clients[client_type](**kwargs)
