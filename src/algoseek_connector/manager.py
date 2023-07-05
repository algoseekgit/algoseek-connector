"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""

from .base import ClientProtocol, DataSource
from .clickhouse.client import ClickHouseClient


class ResourceManager:
    """Manage data sources available to an user."""

    def __init__(self):
        self._client_factory = ClientProtocolFactory()

    def create_data_source(self, name: str, **kwargs) -> DataSource:
        """Create a connection to a data source."""
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
