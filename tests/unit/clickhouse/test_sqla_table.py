import pytest
from clickhouse_sqlalchemy import types as clickhouse_types
from sqlalchemy import types as sqla_types

from algoseek_connector.base import ColumnDescription
from algoseek_connector.clickhouse import sqla_table


@pytest.fixture
def column_factory():
    return sqla_table.SQLAlchemyColumnFactory()


@pytest.mark.parametrize(
    "type_str,enum_values",
    [
        ["Enum('value1' = 1, 'value2' = 2, 'value3' = 3)", [1, 2, 3]],
        ["Enum8('value1' = 10, 'value2' = 30)", [10, 30]],
        ["Enum16('value1' = 10)", [10]],
    ],
)
def test_SQLAlchemyColumnFactory_create_enum_column(column_factory, type_str, enum_values):
    expected_name = "MyEnum"
    expected_description = "MyEnumColumnDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.name == expected_name
    assert actual.doc == expected_description
    assert not actual.nullable
    for member, expected_value in zip(actual.type.enum_class, enum_values):
        assert member.value == expected_value


@pytest.mark.parametrize("type_str", ["Int16", "Int32", "UInt8", "UInt64"])
def test_SQLAlchemyColumnFactory_create_integer_column(column_factory, type_str: str):
    expected_name = "myIntegerColumn"
    expected_description = "MyEnumIntDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    expected_type = getattr(clickhouse_types, type_str)
    assert actual.doc == expected_description
    assert isinstance(actual.type, expected_type)
    assert actual.name == expected_name
    assert not actual.nullable


@pytest.mark.parametrize("type_str", ["Float32", "Float64"])
def test_SQLAlchemyColumnFactory_create_float_column(column_factory, type_str):
    expected_name = "myFloatColumn"
    expected_description = "MyFloatDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, sqla_types.Float)
    assert actual.name == expected_name
    assert not actual.nullable


@pytest.mark.parametrize(
    "type_str,expected_precision,expected_scale",
    [("Decimal(12, 4)", 12, 4), ("Decimal(18, 6)", 18, 6), ("Decimal32(8)", 32, 8)],
)
def test_SQLAlchemyColumnFactory_create_decimal_column(column_factory, type_str, expected_precision, expected_scale):
    expected_name = "myDecimalColumn"
    expected_description = "MyDecimalDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, clickhouse_types.Decimal)
    assert actual.name == expected_name
    assert actual.type.scale == expected_scale
    assert actual.type.precision == expected_precision
    assert not actual.nullable


@pytest.mark.parametrize("type_str,expected_length", [("String", None), ("FixedString(30)", 30)])
def test_SQLAlchemyColumnFactory_create_string_column(column_factory, type_str, expected_length):
    expected_name = "myStringColumn"
    expected_description = "MyStringDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, sqla_types.String)
    assert actual.name == expected_name
    assert not actual.nullable
    if expected_length is not None:
        assert actual.type.length == expected_length


def test_SQLAlchemyColumnFactory_create_date_column(column_factory):
    expected_name = "myStringColumn"
    expected_type = "Date"
    expected_description = ""
    metadata = ColumnDescription(expected_name, expected_type, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, sqla_types.Date)
    assert actual.name == expected_name
    assert not actual.nullable


@pytest.mark.parametrize(
    "type_str,expected_timezone",
    [
        ("DateTime('US/Eastern')", "US/Eastern"),
        ("DateTime('US/Eastern')", "US/Eastern"),
        ("DateTime", None),
    ],
)
def test_SQLAlchemyColumnFactory_create_datetime_column(column_factory, type_str, expected_timezone):
    expected_name = "myDateTimeColumn"
    expected_description = "MyDateDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, clickhouse_types.DateTime)
    assert actual.type.timezone == expected_timezone
    assert not actual.nullable


@pytest.mark.parametrize(
    "type_str,expected_precision,expected_timezone",
    [
        ("DateTime64(3, 'US/Eastern')", 3, "US/Eastern"),
        ("DateTime64(6, 'US/Eastern')", 6, "US/Eastern"),
        ("DateTime64(9)", 9, None),
    ],
)
def test_SQLAlchemyColumnFactory_create_datetime64_column(
    column_factory, type_str, expected_precision, expected_timezone
):
    expected_name = "myDateTimeColumn"
    expected_description = "MyDate64Description"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, clickhouse_types.DateTime64)
    assert actual.type.timezone == expected_timezone
    assert actual.type.precision == expected_precision
    assert not actual.nullable


@pytest.mark.parametrize(
    "type_str,expected_type",
    [
        ("LowCardinality(FixedString(12))", "FixedString(12)"),
        ("LowCardinality(String)", "String"),
    ],
)
def test_SQLAlchemyColumnFactory_create_low_cardinality_column(column_factory, type_str, expected_type):
    expected_name = "myLowCardinalityColumn"
    expected_description = "myLowCardinalityDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert isinstance(actual.type, clickhouse_types.LowCardinality)
    assert isinstance(actual.type.nested_type, clickhouse_types.String)
    assert actual.name == expected_name
    assert actual.doc == expected_description
    assert not actual.nullable


@pytest.mark.parametrize(
    "type_str,expected_type",
    [("Nullable(String)", "String"), ("Nullable(Float64)", "Float64")],
)
def test_SQLAlchemyColumnFactory_nullable_column(column_factory, type_str, expected_type):
    expected_name = "myNullableColumn"
    expected_description = "myNullableDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    expected_nested_type = getattr(clickhouse_types, expected_type)
    assert isinstance(actual.type, clickhouse_types.Nullable)
    assert isinstance(actual.type.nested_type, expected_nested_type)
    assert actual.name == expected_name
    assert actual.doc == expected_description
    assert actual.nullable


@pytest.mark.parametrize(
    "type_str,expected_type",
    [("Array(String)", "String"), ("Array(Float64)", "Float64")],
)
def test_SQLAlchemyColumnFactory_array_column(column_factory, type_str, expected_type):
    expected_name = "myArrayColumn"
    expected_description = "myArrayDescription"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    expected_item_type = getattr(clickhouse_types, expected_type)
    assert isinstance(actual.type, clickhouse_types.Array)
    assert actual.type.item_type is expected_item_type
    assert actual.name == expected_name
    assert actual.doc == expected_description
    assert not actual.nullable


def test_SQLAlchemyColumnFactory_create_boolean_column(column_factory):
    expected_name = "myBooleanTimeColumn"
    expected_description = "myBooleanDescription"
    type_str = "Bool"
    metadata = ColumnDescription(expected_name, type_str, expected_description)
    actual = column_factory(metadata)
    assert actual.doc == expected_description
    assert isinstance(actual.type, clickhouse_types.Boolean)
    assert not actual.nullable


def test_SQLAlchemyColumnFactory_unsupported_type_column(column_factory):
    col_name = "myDateTimeColumn"
    type_str = "Nested(Field1 Int64, Field2 String)"
    col_description = ""
    metadata = ColumnDescription(col_name, type_str, col_description)
    with pytest.raises(sqla_table.UnsupportedClickHouseType):
        column_factory(metadata)


def test_SQLAlchemyColumnFactory_invalid_type_column(column_factory):
    col_name = "myDateTimeColumn"
    type_str = "InvalidType"
    col_description = ""
    metadata = ColumnDescription(col_name, type_str, col_description)
    with pytest.raises(ValueError):
        column_factory(metadata)
