"""
Tools to communicate with Algoseek API metadata.

Provides:

APIConsumer : fetch DB table metadata.

"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

from ..metadata_api import BaseAPIConsumer
from .base import ColumnMetadata, TableMetadata


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

    def get_db_table_metadata(self, group: str, name: str) -> TableMetadata:
        """
        Get the metadata from a table.

        Parameters
        ----------
        group : str
            The group to which the table belongs.
        name : str
            The table name.

        Returns
        -------
        TableMetadata

        Raises
        ------
        ValueError
            If a non-available group or table name is passed.

        """
        self._validate_table(group, name)
        group_metadata = self._get_db_metadata()[group]
        table_dict = group_metadata[name]
        group, name = table_dict["table_name"].split(".")
        column_metadata = [
            _create_column_metadata(x) for x in table_dict["sql_columns"]
        ]
        return TableMetadata(name, group, column_metadata)

    def list_db_groups(self) -> list[str]:
        """List available DB groups."""
        return list(self._get_db_metadata())

    def list_db_tables(self, group: str) -> list[str]:
        """
        List available DB tables.

        Parameters
        ----------
        group : str
            List databases inside this group.

        Raises
        ------
        ValueError
            If an non-available group name is passed.

        """
        self._validate_group(group)
        return list(self._get_db_metadata()[group])

    @lru_cache
    def _get_db_metadata(self) -> dict[str, dict[str, dict]]:
        """List available tables."""
        db_metadata_endpoint = "public/database_table/"
        response = self.get(db_metadata_endpoint)
        db_metadata: dict[str, dict[str, dict]] = dict()
        for table in response.json():
            group, name = table["table_name"].split(".")
            group_metadata = db_metadata.setdefault(group, dict())
            group_metadata[name] = table
        return db_metadata

    def _validate_group(self, group: str):
        """Raise ValueError if an invalid group is passed."""
        valid_groups = self.list_db_groups()
        if group not in valid_groups:
            msg = f"{group} is not a valid data group. Valid groups are one of {valid_groups}"
            raise ValueError(msg)

    def _validate_table(self, group: str, table: str):
        """Raise ValueError if an invalid group is passed."""
        valid_tables = self.list_db_tables(group)
        if table not in valid_tables:
            msg = f"{table} is not a valid table. Valid tables for group {group} are one of {valid_tables}"
            raise ValueError(msg)

    def dump_table_metadata(self, path: str):
        """
        Store table metadata in a JSON file.

        Parameters
        ----------
        path : str
            Path to JSON file.

        """
        with open(path, "wt") as f:
            json.dump(self._get_db_metadata(), f)


class MockAPIConsumer(APIConsumer):
    """Mock class for tests."""

    def __init__(
        self,
        user: Optional[str] = None,
        password: Optional[str] = None,
        data_path: Optional[Path] = None,
    ):
        if data_path is None:
            project_root_path = Path(__file__).parent.parent.parent.parent
            data_path = project_root_path / "tests/data/table_data.json"
        self.data_path = data_path

    @lru_cache
    def _get_db_metadata(self):
        with open(self.data_path) as fin:
            db_metadata = json.load(fin)
        return db_metadata


def _create_column_metadata(metadata: dict) -> ColumnMetadata:
    """
    Extract column metadata obtained from API.

    Helper function to _create_table_metadata.
    """
    name = metadata["name"]
    db_type = metadata["data_type_db"]
    description = metadata["description"]
    return ColumnMetadata(name=name, type=db_type, description=description)
