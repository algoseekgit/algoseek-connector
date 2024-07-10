"""
The connector library for Algoseek Datasets.

For getting started with the library, see the following link:

https://algoseek-connector.readthedocs.io/en/latest/index.html

"""

from . import base, clickhouse, s3, utils
from .manager import ResourceManager

__all__ = ["base", "clickhouse", "ResourceManager", "s3", "utils"]
