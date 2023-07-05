"""
Provides tools to work with Algoseek Datasets.

Datasets access is done via SQL queries, using either method cascading or raw
SQL queries.

TODO: list user classes.
TODO: add examples.

"""

from . import base, clickhouse
from .manager import ResourceManager
from .resources import Datagroup, DataResource, Dataset
from .sessions import ExecutionError, Session

__all__ = [
    "base",
    "Session",
    "ExecutionError",
    "DataResource",
    "Datagroup",
    "Dataset",
    "clickhouse",
    "ResourceManager",
]
