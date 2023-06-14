"""
Provides tools to work with Algoseek Datasets.

Datasets access is done via SQL queries, using either method cascading or raw
SQL queries.

TODO: list user classes.
TODO: add examples.

"""

from .sessions import Session, ExecutionError
from .resources import DataResource, Datagroup, Dataset
from . import base
from . import clickhouse

__all__ = [
    "base",
    "Session",
    "ExecutionError",
    "DataResource",
    "Datagroup",
    "Dataset",
    "clickhouse",
]
