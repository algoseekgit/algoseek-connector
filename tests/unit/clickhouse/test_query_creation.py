import pytest

from algoseek_connector.base import DataGroup, DataSet
from algoseek_connector.clickhouse import sqla_table
from algoseek_connector.clickhouse.base import ColumnMetadata, TableMetadata
from algoseek_connector.clickhouse.resources import MockClickHouseDataResource


@pytest.fixture(scope="module")
def resource():
    resource = MockClickHouseDataResource()
    name = "t"
    group = "g"
    columns = [
        ColumnMetadata("col1", "Float64", ""),
        ColumnMetadata("col2", "Int64", ""),
        ColumnMetadata("col3", "DateTime64(3, 'Asia/Istanbul')", ""),
        ColumnMetadata("col4", "Enum8('A' = 1, 'B' = 2, 'C' = 3)", ""),
    ]
    table_metadata = TableMetadata(name, group, columns)
    group = DataGroup(resource)
    resource.groups["g"] = group
    table_factory = sqla_table.SQLAlchemyTableFactory()
    table = table_factory(table_metadata, group.metadata)
    DataSet(group, table, resource)
    return resource


def remove_new_lines(query: str) -> str:
    return query.replace("\n", "")


def test_select_one_column(resource: MockClickHouseDataResource):
    expected = "SELECT g.t.col1 FROM g.t"
    group_name = "g"
    dataset_name = "t"
    dataset = resource.get_dataset(group_name, dataset_name)
    stmt = dataset.select(dataset.col1)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert expected == actual


def test_select_two_columns(resource: MockClickHouseDataResource):
    expected = "SELECT g.t.col1, g.t.col2 FROM g.t"
    group_name = "g"
    dataset_name = "t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(c.col1, c.col2)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert expected == actual


def test_select_all_columns(resource: MockClickHouseDataResource):
    expected = "SELECT g.t.col1, g.t.col2, g.t.col3, g.t.col4 FROM g.t"
    group_name = "g"
    dataset_name = "t"
    dataset = resource.get_dataset(group_name, dataset_name)
    stmt = dataset.select()
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert expected == actual


def test_select_exclude_columns(resource: MockClickHouseDataResource):
    expected = "SELECT g.t.col1, g.t.col4 FROM g.t"
    group_name = "g"
    dataset_name = "t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(exclude=(c.col2, c.col3))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert expected == actual


def test_select_exclude_all_columns_raise_value_error(
    resource: MockClickHouseDataResource,
):
    group_name = "g"
    dataset_name = "t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    with pytest.raises(ValueError):
        dataset.select(exclude=(c.col1, c.col2, c.col3, c.col4))


def test_select_where(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1, g.t.col3 FROM g.t WHERE g.t.col2 = %(col2_1)s"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    col2_filter = 2
    stmt = dataset.select(c.col1, c.col3).where(c.col2 == col2_filter)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters["col2_1"] == col2_filter
