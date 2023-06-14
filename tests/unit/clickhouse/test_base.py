import pytest
from algoseek_connector.clickhouse import base


@pytest.mark.parametrize("type_str", ["UInt64", "DateTime", "Date", "Integer"])
def test_ColumnMetadata_get_type_name_no_args(type_str):
    column_name = "ColumnName"
    description = "column description"
    column_metadata = base.ColumnMetadata(column_name, type_str, description)
    actual_type_name = column_metadata.get_type_name()
    expected_type_name = type_str
    assert actual_type_name == expected_type_name


@pytest.mark.parametrize(
    "type_str,expected_name,expected_args",
    [
        ("Decimal(12, 4)", "Decimal", ["12", "4"]),
        ("DateTime64(9, 'US/Eastern')", "DateTime64", ["9", "'US/Eastern'"]),
        (
            "Enum('TRADE' = 1, 'TRADE NB' = 2, 'TRADE CANCELLED' = 3)",
            "Enum",
            ["'TRADE' = 1", "'TRADE NB' = 2", "'TRADE CANCELLED' = 3"],
        ),
    ],
)
def test_parse_valid_type_with_args(type_str, expected_name, expected_args):
    column_name = "ColumnName"
    description = "column description"
    column_metadata = base.ColumnMetadata(column_name, type_str, description)
    actual_args = column_metadata.get_type_args()
    assert actual_args == expected_args
