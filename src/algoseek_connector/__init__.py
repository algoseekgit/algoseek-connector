"""
The connector library for Algoseek Datasets.

For getting started with the library, see the following link at TODO: ADD LINK.

Datasets access is done via SQL queries, using either method cascading or raw
SQL queries.

TODO: list user classes.
TODO: add examples.

"""

from . import base, clickhouse, s3, utils
from .manager import ResourceManager

__all__ = ["base", "clickhouse", "ResourceManager", "s3", "utils"]
