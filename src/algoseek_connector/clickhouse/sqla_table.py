"""Tools to create SQLAlchemy Tables from base.TableMetadata."""

import enum
from typing import cast

import numpy as np
from clickhouse_sqlalchemy import types as clickhouse_types
from clickhouse_sqlalchemy.types.common import ClickHouseTypeEngine
from numpy.typing import DTypeLike
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.types import TypeEngine

from . import base
from .base import ColumnMetadata, TableMetadata


class SQLAlchemyTableFactory:
    """Create SQLAlchemy tables from table metadata."""

    def __init__(self):
        self.column_factory = SQLAlchemyColumnFactory()

    def __call__(self, table_metadata: TableMetadata, metadata: MetaData) -> Table:
        """
        Create a SQLAlchemy Table from a TableMetadata instance.

        Parameters
        ----------
        table_metadata : TableMetadata
        metadata : sqlalchemy.MetaData
            Stores a collection of related tables.

        Returns
        -------
        sqlalchemy.Table

        """
        columns = [self.column_factory(x) for x in table_metadata.columns]
        return Table(table_metadata.get_table_name(), metadata, *columns, quote=False)


class SQLAlchemyColumnFactory:
    """Create SQLAlchemy columns using column metadata."""

    def __init__(self):
        self.type_mapper = ClickHouseTypeMapper()

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
        ValueError
            If an invalid type string is passed.

        """
        name = column_metadata.name
        T = self.type_mapper.get_type(column_metadata)
        nullable = isinstance(T, clickhouse_types.Nullable)
        doc = column_metadata.description
        return Column(name, T, nullable=nullable, doc=doc, quote=False)


class ClickHouseTypeMapper:
    """Search Column types using a string representation of the type."""

    def __init__(self):
        self.clickhouse_types = base.ClickHouseTypes()

    def get_type(self, column_metadata: ColumnMetadata) -> TypeEngine:
        """
        Search a ClickHouse type.

        Parameters
        ----------
        column_metadata : ColumnMetadata

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
        self.clickhouse_types.fix_type(column_metadata)
        t = column_metadata.get_type_name()
        if t == self.clickhouse_types.ARRAY:
            T = self._to_array(column_metadata)
        elif t == self.clickhouse_types.BOOLEAN:
            # This is type is specified here because the type is named
            # incorrectly on clickhouse-sqlalchemy
            T = clickhouse_types.Boolean()
        elif t == self.clickhouse_types.DATETIME:
            T = self._to_datetime(column_metadata)
        elif t == self.clickhouse_types.DATETIME64:
            T = self._to_datetime64(column_metadata)
        elif t in self.clickhouse_types.DECIMAL:
            T = self._to_decimal(column_metadata)
        elif t in self.clickhouse_types.ENUM:
            T = self._to_enum(column_metadata)
        elif t == self.clickhouse_types.FIXED_STRING:
            T = self._to_fixed_string(column_metadata)
        elif t == self.clickhouse_types.LOW_CARDINALITY:
            T = self._to_low_cardinality(column_metadata)
        elif t == self.clickhouse_types.NULLABLE:
            T = self._to_nullable(column_metadata)
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

    def _to_array(self, column_metadata: ColumnMetadata) -> clickhouse_types.Array:
        inner_type_str = column_metadata.get_type_args()[0]
        inner = ColumnMetadata(column_metadata.name, inner_type_str, "")
        T = self.get_type(inner)
        return clickhouse_types.Array(T)

    def _to_datetime(
        self, column_metadata: ColumnMetadata
    ) -> clickhouse_types.DateTime:
        type_args = column_metadata.get_type_args()
        timezone = type_args[0].strip("'") if type_args else None
        # TODO: cast fixes an incorrect type annotation in clickhouse-sqlalchemy
        return clickhouse_types.DateTime(cast(bool, timezone))

    def _to_datetime64(
        self, column_metadata: ColumnMetadata
    ) -> clickhouse_types.DateTime:
        type_args = column_metadata.get_type_args()
        if len(type_args) == 2:
            precision, timezone = type_args
            timezone = timezone.strip("'")
        else:
            precision = type_args[0]
            timezone = None
        return clickhouse_types.DateTime64(int(precision), timezone)

    def _to_decimal(self, column_metadata: ColumnMetadata) -> clickhouse_types.Decimal:
        t = column_metadata.get_type_name()
        if t == "Decimal":
            precision, scale = column_metadata.get_type_args()
        else:
            scale = column_metadata.get_type_args()[0]
            precision = [x for x in ["32", "64", "128", "256"] if x in t][0]
        return clickhouse_types.Decimal(int(precision), int(scale))

    def _to_enum(self, column_metadata: ColumnMetadata) -> clickhouse_types.Enum:
        members = dict()
        args = column_metadata.get_type_args()
        for arg in args:
            member, value = arg.split("=")
            member = member.strip("' ")
            members[member] = int(value.strip())
        python_enum = enum.Enum(column_metadata.name, members)
        ch_enum_class = getattr(clickhouse_types, column_metadata.get_type_name())
        return ch_enum_class(python_enum)

    def _to_fixed_string(
        self, column_metadata: ColumnMetadata
    ) -> clickhouse_types.String:
        length = int(column_metadata.get_type_args()[0])
        return clickhouse_types.String(length)

    def _to_nullable(
        self, column_metadata: ColumnMetadata
    ) -> clickhouse_types.Nullable:
        inner_type_str = column_metadata.get_type_args()[0]
        inner = ColumnMetadata(column_metadata.name, inner_type_str, "")
        T = self.get_type(inner)
        return clickhouse_types.Nullable(T)

    def _to_low_cardinality(
        self, column_metadata: ColumnMetadata
    ) -> clickhouse_types.LowCardinality:
        inner_type_str = column_metadata.get_type_args()[0]
        inner = ColumnMetadata(column_metadata.name, inner_type_str, "")
        T = self.get_type(inner)
        return clickhouse_types.LowCardinality(T)


class ClickHouseToNumpyTypeMapper:
    """Map ClickHouse Types to Numpy types."""

    def __init__(self):
        self.basic = {
            "UInt8": np.uint8,
            "UInt16": np.uint16,
            "UInt32": np.uint32,
            "UInt64": np.uint64,
            # "UInt128": np.uint128,
            # "UInt256": np.uint256,
            "Int8": np.int8,
            "Int16": np.int16,
            "Int32": np.int32,
            "Int64": np.int64,
            # "Int128": np.int128,
            # "Int256": np.int256,
            "Float32": np.float32,
            "Float64": np.float64,
            "Bool": np.bool_,
            "String": np.str_,
            "Date": np.datetime64,
            "Date32": np.datetime64,
            "DateTime": np.datetime64,
            "DateTime64": np.datetime64,
        }

        self.nested = {"LowCardinality", "Nullable", "Array"}

    def get_type(self, T: ClickHouseTypeEngine) -> DTypeLike:
        """
        Search a Numpy Type.

        Parameters
        ----------
        T : ClickHouseTypeEngine

        Returns
        -------
        DTypeLike
            Numpy DType

        Raises
        ------
        ValueError
            If an unsupported ClickHouse type is passed.

        """
        name = str(T)
        if name in self.basic:
            numpy_type = self.basic[name]
        elif hasattr(T, "nested_type"):
            numpy_type = self.basic[getattr(T, "nested_type")]
        elif name in ["Enum", "Enum8", "Enum16"]:
            numpy_type = np.uint16
        elif name == "Decimal":
            precision = getattr(T, "precision")
            numpy_type_str = f"Float{precision}"
            numpy_type = self.basic[numpy_type_str]
        else:
            msg = f"{name} has not a corresponding Numpy Dtype assigned."
            raise ValueError(msg)
        return numpy_type


class UnsupportedClickHouseType(ValueError):
    """Exception class to raise when an unsupported ClickHouse type is used."""

    pass
