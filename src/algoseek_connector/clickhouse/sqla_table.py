"""Tools to create SQLAlchemy Tables from base.TableMetadata."""

import enum
from sqlalchemy import types, Column, Table, MetaData
from typing import Callable, Optional
from . import base
from .base import ColumnMetadata, TableMetadata

SQLALCHEMY_DECIMAL = "Decimal"
SQLALCHEMY_FLOAT = "Float"
SQLALCHEMY_INT = "Integer"
SQLALCHEMY_ENUM = "Enum"
SQLALCHEMY_DATE = "Date"
SQLALCHEMY_DATETIME = "DateTime"
SQLALCHEMY_STRING = "String"
SQLALCHEMY_BOOLEAN = "Boolean"


class DateTime(types.TypeDecorator):
    """SQLAlchemy Type for ClickHouse DateTime."""

    impl = types.DateTime

    def __init__(self, timezone: Optional[str] = None):
        self.timezone = timezone


class DateTime64(types.TypeDecorator):
    """SQLAlchemy Type for ClickHouse DateTime64."""

    impl = types.DateTime

    def __init__(self, precision: int, timezone: Optional[str] = None):
        self.precision = precision
        self.timezone = timezone


class UnsupportedClickHouseType(ValueError):
    """Exception class to raise when an unsupported ClickHouse type is used."""

    pass


class SQLAlchemyColumnFactory:
    """Create SQLAlchemy columns using column metadata."""

    def __init__(self):
        self.ch_type_to_sqla_type = _get_clickhouse_type_to_sqlalchemy_type_mapping()
        self.sqla_type_to_column_creator: dict[
            str, Callable[[ColumnMetadata], Column]
        ] = {
            SQLALCHEMY_DATE: _to_date,
            SQLALCHEMY_DATETIME: _to_datetime,
            SQLALCHEMY_DECIMAL: _to_decimal,
            SQLALCHEMY_FLOAT: _to_float,
            SQLALCHEMY_INT: _to_integer,
            SQLALCHEMY_ENUM: _to_enum,
            SQLALCHEMY_STRING: _to_string,
            SQLALCHEMY_BOOLEAN: _to_boolean,
        }

    def __call__(self, column_metadata: ColumnMetadata) -> Column:
        """
        Create a a SQLAlchemy column using column metadata.

        Parameters
        ----------
        column_metadata : ColumnMetadata

        Returns
        -------
        sqlalchemy.Column

        Raises
        ------
        UnsupportedClickHouseType
            If an unsupported type is used.

        """
        if column_metadata.get_type_name() == base.CLICKHOUSE_LOW_CARDINALITY:
            args = column_metadata.get_type_args()
            column_type = args[0]
            column_metadata = ColumnMetadata(
                column_metadata.name, column_type, column_metadata.description
            )

        try:
            sqla_type = self.ch_type_to_sqla_type[column_metadata.get_type_name()]
            create_column = self.sqla_type_to_column_creator[sqla_type]
            column = create_column(column_metadata)
        except KeyError:
            msg = f"{column_metadata.type} type is not supported."
            raise UnsupportedClickHouseType(msg)

        return column


class SQLAlchemyTableFactory:
    """Create SQLAlchemy tables from table metadata."""

    def __init__(self):
        self.column_factory = SQLAlchemyColumnFactory()

    def __call__(self, table_metadata: TableMetadata, metadata: MetaData) -> Table:
        """Create a SQLAlchemy Table from a TableMetadata instance."""
        columns = [self.column_factory(x) for x in table_metadata.columns]
        return Table(table_metadata.name, metadata, *columns)


def _get_clickhouse_type_to_sqlalchemy_type_mapping() -> dict[str, str]:
    sqla_type_to_clickhouse_type = {
        SQLALCHEMY_FLOAT: base.CLICKHOUSE_FLOAT,
        SQLALCHEMY_INT: base.CLICKHOUSE_INTEGER,
        SQLALCHEMY_DECIMAL: base.CLICKHOUSE_DECIMAL,
        SQLALCHEMY_DATE: base.CLICKHOUSE_DATE,
        SQLALCHEMY_DATETIME: base.CLICKHOUSE_DATETIME,
        SQLALCHEMY_ENUM: base.CLICKHOUSE_ENUM,
        SQLALCHEMY_STRING: base.CLICKHOUSE_STRING,
        SQLALCHEMY_BOOLEAN: base.CLICKHOUSE_BOOLEAN,
    }

    clickhouse_type_to_sqla_type = dict()
    for sqla_type, clickhouse_types in sqla_type_to_clickhouse_type.items():
        for cht in clickhouse_types:
            clickhouse_type_to_sqla_type[cht] = sqla_type

    return clickhouse_type_to_sqla_type


def _to_enum(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA Enum type from column metadata."""
    members = dict()
    # Enums are assumed to have the format described in
    # https://clickhouse.com/docs/en/sql-reference/data-types/enum
    args = column_metadata.get_type_args()
    for arg in args:
        member, value = arg.split("=")
        member = member.strip("' ")
        value = int(value.strip())
        members[member] = value
    column_type = types.Enum(enum.Enum(column_metadata.name, members))
    return Column(column_metadata.name, column_type, doc=column_metadata.description)


def _to_integer(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA Int Column from column metadata."""
    return Column(column_metadata.name, types.Integer, doc=column_metadata.description)


def _to_float(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA Float Column from column metadata."""
    return Column(column_metadata.name, types.Float, doc=column_metadata.description)


def _to_decimal(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA Decimal Column from column metadata."""
    args = column_metadata.get_type_args()
    if len(args) == 1:
        scale = int(args[0])
        # extract precision value from name
        decimal_precision = ["32", "64", "128", "256"]
        precision = None
        for p in decimal_precision:
            if p in column_metadata.get_type_name():
                precision = int(p)
                break

    else:
        precision, scale = int(args[0]), int(args[1])
    return Column(
        column_metadata.name,
        types.DECIMAL(precision, scale),
        doc=column_metadata.description,
    )


def _to_boolean(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA Bool Column from column metadata."""
    return Column(column_metadata.name, types.Boolean, doc=column_metadata.description)


def _to_string(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA String Column from column metadata."""
    args = column_metadata.get_type_args()
    if args:
        size = int(args[0])
        string_type = types.String(size)
    else:
        string_type = types.String
    return Column(column_metadata.name, string_type, doc=column_metadata.description)


def _to_date(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLA Date Column from column metadata."""
    return Column(column_metadata.name, types.Date, doc=column_metadata.description)


def _to_datetime(column_metadata: ColumnMetadata) -> Column:
    """Create a SQLAlchemy DateTime Column from column metadata."""
    args = column_metadata.get_type_args()
    if column_metadata.get_type_name() == "DateTime64":
        if len(args) == 2:
            precision, timezone = args
            precision = int(precision)
            timezone = timezone.strip("'")
        else:
            precision = int(args[0])
            timezone = None
        column_type = DateTime64(precision, timezone)
    else:
        if args:
            timezone = args[0].strip("'")
        else:
            timezone = None
        column_type = DateTime(timezone)
    return Column(column_metadata.name, column_type, doc=column_metadata.description)
