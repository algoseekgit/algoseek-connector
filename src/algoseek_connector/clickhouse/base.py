"""
Base functions, objects and constants.

Provides:

ColumnMetadata : Stores column name, data type and description.
TableMetadata : Stores table name and column metadata.

"""

import dataclasses
import json
from pathlib import Path


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

    def get_table_name(self) -> str:
        """Get the table name in the format `group.name`."""
        return f"{self.group}.{self.name}"


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

    LOW_CARDINALITY = "LowCardinality"
    NULLABLE = "Nullable"
    ARRAY = ["Array"]
    STRING = ["String", "FixedString"]
    BOOLEAN = ["Bool"]
    DATETIME = ["DateTime", "DateTime64"]
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

    def fix_type(self, column_metadata: ColumnMetadata):
        """Replace case-insensitive and aliased types with the internally used type."""
        type_str = column_metadata.get_type_name()
        upper = type_str.upper()
        if upper in self.CASE_INSENSITIVE:
            fixed = self.CASE_INSENSITIVE[upper]
            fixed = self.ALIAS.get(fixed, fixed)  # replace by alias if available
        elif type_str in self.ALIAS:
            fixed = self.ALIAS[type_str]
        else:
            fixed = type_str
        offset = len(type_str)
        column_metadata.type = fixed + column_metadata.type[offset:]
