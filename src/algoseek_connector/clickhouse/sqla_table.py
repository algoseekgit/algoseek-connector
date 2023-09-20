"""Tools to create SQLAlchemy Tables from base.TableMetadata."""

import dataclasses
import enum
import json
from pathlib import Path
from typing import cast

from clickhouse_sqlalchemy import types as clickhouse_types
from clickhouse_sqlalchemy.types.common import ClickHouseTypeEngine
from sqlalchemy import Column
from sqlalchemy.types import TypeEngine

from ..base import ColumnDescription


class SQLAlchemyColumnFactory:
    """Create SQLAlchemy columns using column descriptions."""

    def __init__(self):
        self.type_mapper = ClickHouseTypeMapper()

    def __call__(self, description: ColumnDescription) -> Column:
        """
        Create a a SQLAlchemy column from a column description.

        Parameters
        ----------
        description : ColumnDescription

        Returns
        -------
        sqlalchemy.Column

        Raises
        ------
        UnsupportedClickHouseType
            If an unsupported type is used.
        ValueError
            If an invalid type string is passed.

        """
        name = description.name
        T = self.type_mapper.get_type(description)
        nullable = isinstance(T, clickhouse_types.Nullable)
        doc = description.description
        return Column(name, T, nullable=nullable, doc=doc, quote=False)


def _alias_dict() -> dict[str, str]:
    """Create a dictionary that map aliased types to their alias."""
    json_path = Path(__file__).parent / "alias.json"
    with open(json_path) as fin:
        alias = json.load(fin)
    return alias


def _case_insensitive_dict() -> dict[str, str]:
    """Create a dictionary that contains case insensitive types."""
    json_path = Path(__file__).parent / "case_insensitive.json"
    with open(json_path) as fin:
        case_insensitive = json.load(fin)
    return case_insensitive


@dataclasses.dataclass(frozen=True)
class ClickHouseTypes:
    """Store string representation of ClickHouse Types."""

    UNSUPPORTED = ["Nested", "Map", "Tuple"]
    LOW_CARDINALITY = "LowCardinality"
    NULLABLE = "Nullable"
    ARRAY = "Array"
    STRING = "String"
    FIXED_STRING = "FixedString"
    BOOLEAN = "Bool"
    DATETIME = "DateTime"
    DATETIME64 = "DateTime64"
    DATE = ["Date", "Date32"]
    DECIMAL = ["Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256"]
    ENUM = ["Enum", "Enum8", "Enum16"]
    FLOAT = ["Float32", "Float64"]
    INTEGER = [
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "UInt128",
        "UInt256",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Int128",
        "Int256",
    ]
    ALIAS = _alias_dict()
    CASE_INSENSITIVE = _case_insensitive_dict()

    def fix_type(self, description: ColumnDescription):
        """Replace case-insensitive and aliased types with the internally used type."""
        type_str = description.get_type_name()
        upper = type_str.upper()
        if upper in self.CASE_INSENSITIVE:
            fixed = self.CASE_INSENSITIVE[upper]
            fixed = self.ALIAS.get(fixed, fixed)  # replace by alias if available
        elif type_str in self.ALIAS:
            fixed = self.ALIAS[type_str]
        else:
            fixed = type_str
        offset = len(type_str)
        description.type = fixed + description.type[offset:]


class ClickHouseTypeMapper:
    """Search Column types using a string representation of the type."""

    def __init__(self):
        self.clickhouse_types = ClickHouseTypes()

    def get_type(self, column_description: ColumnDescription) -> TypeEngine:
        """
        Search a ClickHouse type.

        Parameters
        ----------
        column_description : ColumnDescription

        Returns
        -------
        TypeEngine

        Raises
        ------
        UnsupportedClickHouseType
            If an unsupported type is used.
        ValueError
            If an invalid type string is passed.

        """
        self.clickhouse_types.fix_type(column_description)
        t = column_description.get_type_name()
        if t == self.clickhouse_types.ARRAY:
            T = self._to_array(column_description)
        elif t == self.clickhouse_types.BOOLEAN:
            # This is type is specified here because the type is named
            # incorrectly on clickhouse-sqlalchemy
            T = clickhouse_types.Boolean()
        elif t == self.clickhouse_types.DATETIME:
            T = self._to_datetime(column_description)
        elif t == self.clickhouse_types.DATETIME64:
            T = self._to_datetime64(column_description)
        elif t in self.clickhouse_types.DECIMAL:
            T = self._to_decimal(column_description)
        elif t in self.clickhouse_types.ENUM:
            T = self._to_enum(column_description)
        elif t == self.clickhouse_types.FIXED_STRING:
            T = self._to_fixed_string(column_description)
        elif t == self.clickhouse_types.LOW_CARDINALITY:
            T = self._to_low_cardinality(column_description)
        elif t == self.clickhouse_types.NULLABLE:
            T = self._to_nullable(column_description)
        elif t in self.clickhouse_types.UNSUPPORTED:
            msg = f"{t} is not currently supported."
            raise UnsupportedClickHouseType(msg)
        else:
            try:
                T = cast(ClickHouseTypeEngine, getattr(clickhouse_types, t))
            except AttributeError:
                msg = f"{t} is not a valid ClickHouse Type."
                raise ValueError(msg)
        return T

    def _to_array(self, description: ColumnDescription) -> clickhouse_types.Array:
        inner_type_str = description.get_type_args()[0]
        inner = ColumnDescription(description.name, inner_type_str, "")
        T = self.get_type(inner)
        return clickhouse_types.Array(T)

    def _to_datetime(self, description: ColumnDescription) -> clickhouse_types.DateTime:
        type_args = description.get_type_args()
        timezone = type_args[0].strip("'") if type_args else None
        # cast fixes an incorrect type annotation in clickhouse-sqlalchemy
        return clickhouse_types.DateTime(cast(bool, timezone))

    def _to_datetime64(
        self, description: ColumnDescription
    ) -> clickhouse_types.DateTime:
        type_args = description.get_type_args()
        if len(type_args) == 2:
            precision, timezone = type_args
            timezone = timezone.strip("'")
        else:
            precision = type_args[0]
            timezone = None
        return clickhouse_types.DateTime64(int(precision), timezone)

    def _to_decimal(self, description: ColumnDescription) -> clickhouse_types.Decimal:
        t = description.get_type_name()
        if t == "Decimal":
            precision, scale = description.get_type_args()
        else:
            scale = description.get_type_args()[0]
            precision = [x for x in ["32", "64", "128", "256"] if x in t][0]
        return clickhouse_types.Decimal(int(precision), int(scale))

    def _to_enum(self, description: ColumnDescription) -> clickhouse_types.Enum:
        members = dict()
        args = description.get_type_args()
        for arg in args:
            member, value = arg.split("=")
            member = member.strip("' ")
            members[member] = int(value.strip())
        python_enum = enum.Enum(description.name, members)
        ch_enum_class = getattr(clickhouse_types, description.get_type_name())
        return ch_enum_class(python_enum)

    def _to_fixed_string(
        self, description: ColumnDescription
    ) -> clickhouse_types.String:
        length = int(description.get_type_args()[0])
        return clickhouse_types.String(length)

    def _to_nullable(self, description: ColumnDescription) -> clickhouse_types.Nullable:
        inner_type_str = description.get_type_args()[0]
        inner = ColumnDescription(description.name, inner_type_str, "")
        T = self.get_type(inner)
        return clickhouse_types.Nullable(T)

    def _to_low_cardinality(
        self, description: ColumnDescription
    ) -> clickhouse_types.LowCardinality:
        inner_type_str = description.get_type_args()[0]
        inner = ColumnDescription(description.name, inner_type_str, "")
        T = self.get_type(inner)
        return clickhouse_types.LowCardinality(T)


class UnsupportedClickHouseType(ValueError):
    """Exception class to raise when an unsupported ClickHouse type is used."""

    pass
