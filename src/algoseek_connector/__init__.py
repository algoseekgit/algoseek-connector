"""
Provides tools to work with Algoseek Datasets.

Datasets access is done via SQL queries, using either method cascading or raw
SQL queries.

TODO: list user classes.
TODO: add examples.

"""

from . import base, clickhouse, s3
from .manager import ResourceManager

__all__ = ["base", "clickhouse", "ResourceManager", "s3"]
