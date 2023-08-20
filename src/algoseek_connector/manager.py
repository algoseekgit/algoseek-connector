"""
Tools to connect to different data sources.

Provides:

- ResourceManager
    Creates available data sources.

"""
import os

from . import base, clickhouse, s3
from .metadata_api import AuthToken, BaseAPIConsumer

ARDADB = "ardadb"
S3 = "s3"
ALGOSEEK_AWS_PROFILE = "ALGOSEEK_AWS_PROFILE"
ALGOSEEK_AWS_ACCESS_KEY_ID = "ALGOSEEK_AWS_ACCESS_KEY_ID"
ALGOSEEK_AWS_SECRET_ACCESS_KEY = "ALGOSEEK_AWS_SECRET_ACCESS_KEY"


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
            profile_name = kwargs.get("profile_name", os.getenv(ALGOSEEK_AWS_PROFILE))
            aws_access_key_id = kwargs.get(
                "aws_access_key_id", os.getenv(ALGOSEEK_AWS_ACCESS_KEY_ID)
            )
            aws_secret_access_key = kwargs.get(
                "aws_secret_access_key", os.getenv(ALGOSEEK_AWS_SECRET_ACCESS_KEY)
            )
            session = s3.create_boto3_session(
                profile_name=profile_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            client = s3.S3DownloaderClient(session, self._api)
        else:
            raise ValueError
        return client

    def list_data_sources(self) -> list[str]:
        """List available data sources."""
        sources = [ARDADB, S3]
        return sources
