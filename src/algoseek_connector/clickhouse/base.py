"""
Base functions, objects and constants.

Provides:

ColumnMetadata : Stores column name, data type and description.
TableMetadata : Stores table name and column metadata.

"""

import dataclasses

CLICKHOUSE_BOOLEAN = ["Boolean"]
CLICKHOUSE_DATETIME = ["DateTime", "DateTime64"]
CLICKHOUSE_DATE = ["Date", "Date32"]
CLICKHOUSE_DECIMAL = ["Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256"]
CLICKHOUSE_ENUM = ["Enum"]
CLICKHOUSE_FLOAT = ["Float32", "Float64"]
CLICKHOUSE_INTEGER = [
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
CLICKHOUSE_LOW_CARDINALITY = "LowCardinality"
CLICKHOUSE_NULLABLE = "Nullable"
CLICKHOUSE_STRING = ["String", "FixedString"]


@dataclasses.dataclass
class ColumnMetadata:
    """
    Store column metadata from a ClickHouse table.

    Attributes
    ----------
    name : str
        The column name.
    type : str
        A string representation of the column type.
    description : str
        A short description of the data.

    """

    name: str
    type: str
    description: str

    def __post_init__(self):
        """Validate fields."""
        # TODO: add validation
        pass

    def get_type_name(self) -> str:
        """Get the type name."""
        ind = self.type.find("(")
        if ind == -1:  # no args
            type_name = self.type
        else:
            type_name = self.type[:ind]
        return type_name

    def get_type_args(self) -> list[str]:
        """Get the type arguments."""
        open_ind = self.type.find("(")
        if open_ind != -1:
            close_ind = -1
            type_args = [
                x.strip() for x in self.type[open_ind + 1 : close_ind].split(",")
            ]
        else:
            type_args = list()
        return type_args


@dataclasses.dataclass
class TableMetadata:
    """
    Store metadata from a DB table.

    Attributes
    ----------
    name : str
        The table name.
    group : str
        The data group name.
    columns : list[ColumnMetadata]
        Metadata of each column.

    """

    name: str
    group: str
    columns: list[ColumnMetadata]
