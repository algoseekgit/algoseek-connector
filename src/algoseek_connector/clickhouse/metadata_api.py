"""
Tools to communicate with Algoseek API metadata.

Provides:

APIConsumer : fetch DB table metadata.

"""

from functools import lru_cache
from typing import Any

from ..metadata_api import BaseAPIConsumer


class APIConsumer(BaseAPIConsumer):
    """
    Fetch ClickHouse DB dataset metadata from metadata services.

    Inherits from :py:class:`algoseek_connector.metadata_api.BaseAPIConsumer`.

    Parameters
    ----------
    user : str or None, default=None
        User name for API login. If ``None``, it tries to get the username
        from the environment variable `ALGOSEEK_API_USERNAME`.
    password : str
        Password for API login. If ``None``, it tries to get the username
        from the environment variable `ALGOSEEK_API_PASSWORD`.

    Raises
    ------
    requests.exceptions.HTTPError
        If authentication fails or the connection times out.

    """

    @lru_cache
    def _fetch_dataset_metadata(self) -> dict[str, Any]:
        """
        Fetch metadata from all datasets.

        Returns a dictionary where from text_id of datasets to dataset metadata.
        """
        """Fetch metadata from all datasets."""
        all_metadata = super()._fetch_dataset_metadata()
        db_metadata = dict()
        for k, v in all_metadata.items():
            if "database_table" in v:
                db_metadata[k] = v
        return db_metadata
