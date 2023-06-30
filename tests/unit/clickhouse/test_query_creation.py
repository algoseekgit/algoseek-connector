import pytest
from sqlalchemy import func

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
        ColumnMetadata("col5", "String", ""),
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
    expected = "SELECT g.t.col1, g.t.col2, g.t.col3, g.t.col4, g.t.col5 FROM g.t"
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
    stmt = dataset.select(exclude=(c.col2, c.col3, c.col5))
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
        dataset.select(exclude=(c.col1, c.col2, c.col3, c.col4, c.col5))


def test_select_groupby(resource: MockClickHouseDataResource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT avg(g.t.col1) AS avg_col1 FROM g.t GROUP BY g.t.col4"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(func.avg(c.col1).label("avg_col1")).group_by(c.col4)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_groupby_two_columns(resource: MockClickHouseDataResource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT avg(g.t.col1) AS avg_col1, g.t.col4 FROM g.t GROUP BY g.t.col4"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(func.avg(c.col1).label("avg_col1"), c.col4).group_by(c.col4)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


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


def test_select_where_logical_and(resource):
    group_name = "g"
    dataset_name = "t"
    expected = (
        "SELECT g.t.col1 "
        "FROM g.t "
        "WHERE g.t.col2 = %(col2_1)s AND g.t.col1 >= %(col1_1)s"
    )
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    col2_filter = 2
    col1_filter = 5
    stmt = dataset.select(c.col1).where(
        (c.col2 == col2_filter) & (c.col1 >= col1_filter)
    )
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters["col2_1"] == col2_filter
    assert query.parameters["col1_1"] == col1_filter


def test_select_where_logical_or(resource):
    group_name = "g"
    dataset_name = "t"
    expected = (
        "SELECT g.t.col1 "
        "FROM g.t "
        "WHERE g.t.col2 = %(col2_1)s OR g.t.col1 >= %(col1_1)s"
    )
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    col2_filter = 2
    col1_filter = 5
    stmt = dataset.select(c.col1).where(
        (c.col2 == col2_filter) | (c.col1 >= col1_filter)
    )
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters["col2_1"] == col2_filter
    assert query.parameters["col1_1"] == col1_filter


def test_select_where_in(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1 FROM g.t WHERE g.t.col2 IN (1, 2, 3)"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    col2_filter = [1, 2, 3]
    stmt = dataset.select(c.col1).where(c.col2.in_(col2_filter))
    query = resource.compile(stmt, compile_kwargs={"literal_binds": True})
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_where_between(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1 FROM g.t WHERE g.t.col2 BETWEEN 10 AND 20"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    low = 10
    high = 20
    stmt = dataset.select(c.col1).where(c.col2.between(low, high))
    query = resource.compile(stmt, compile_kwargs={"literal_binds": True})
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_where_like(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1 FROM g.t WHERE g.t.col5 LIKE %(col5_1)s"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    pattern = "pattern%"
    stmt = dataset.select(c.col1).where(c.col5.like(pattern))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters["col5_1"] == pattern


def test_select_order(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1 FROM g.t ORDER BY g.t.col2"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(c.col1).order_by(c.col2)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_order_desc(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1 FROM g.t ORDER BY g.t.col2 DESC"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(c.col1).order_by(c.col2.desc())
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_order_multiple(resource):
    group_name = "g"
    dataset_name = "t"
    expected = "SELECT g.t.col1 FROM g.t ORDER BY g.t.col3, g.t.col2 DESC"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select(c.col1).order_by(c.col3, c.col2.desc())
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_limit(resource):
    group_name = "g"
    dataset_name = "t"
    # TODO: bug in clickhouse-sqlalchemy extra space in LIMIT. make a PR to fix.
    expected = "SELECT g.t.col1, g.t.col2 FROM g.t ORDER BY g.t.col1  LIMIT %(param_1)s"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    limit = 20
    stmt = dataset.select(c.col1, c.col2).order_by(c.col1).limit(limit)
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters["param_1"] == limit


def test_select_arithmetic_add_columns(resource):
    group_name = "g"
    dataset_name = "t"
    col_sum_name = "col_sum"
    expected = f"SELECT g.t.col1 + g.t.col2 AS {col_sum_name} FROM g.t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select((c.col1 + c.col2).label(col_sum_name))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_arithmetic_add_literal(resource):
    group_name = "g"
    dataset_name = "t"
    lit = 5.0
    lit_placeholder = "col1_1"
    col_label = "col1_sum"
    expected = f"SELECT g.t.col1 + %({lit_placeholder})s AS {col_label} FROM g.t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select((c.col1 + lit).label(col_label))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters["col1_1"] == lit
    assert query.parameters[lit_placeholder] == lit


def test_select_arithmetic_multiply_columns(resource):
    group_name = "g"
    dataset_name = "t"
    col_label = "col_prod"
    expected = f"SELECT g.t.col1 * g.t.col2 AS {col_label} FROM g.t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select((c.col1 * c.col2).label(col_label))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_arithmetic_multiply_literal(resource):
    group_name = "g"
    dataset_name = "t"
    col_label = "col_prod"
    lit = 5.0
    lit_placeholder = "col1_1"
    expected = f"SELECT g.t.col1 * %({lit_placeholder})s AS {col_label} FROM g.t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select((c.col1 * lit).label(col_label))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters[lit_placeholder] == lit


def test_select_arithmetic_divide_columns(resource):
    group_name = "g"
    dataset_name = "t"
    col_label = "col_div"
    expected = f"SELECT g.t.col1 / g.t.col2 AS {col_label} FROM g.t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select((c.col1 / c.col2).label(col_label))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_arithmetic_divide_literal(resource):
    group_name = "g"
    dataset_name = "t"
    col_label = "col_div"
    lit_placeholder = "col1_1"
    lit = 5.0
    expected = f"SELECT g.t.col1 / %({lit_placeholder})s AS {col_label} FROM g.t"
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    stmt = dataset.select((c.col1 / lit).label(col_label))
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected


def test_select_complex_query(resource):
    group_name = "g"
    dataset_name = "t"
    having_placeholder = "avg_1"
    where_placeholder = "toMonth_1"
    col2_mean_label = "mean_col2"
    col3_year_label = "year"
    expected = (
        f"SELECT avg(g.t.col2) AS {col2_mean_label}, toYear(g.t.col3) AS {col3_year_label} "
        "FROM g.t "
        f"WHERE toMonth(g.t.col3) = %({where_placeholder})s "
        "GROUP BY toYear(g.t.col3) "
        f"HAVING avg(g.t.col2) > %({having_placeholder})s"
    )
    dataset = resource.get_dataset(group_name, dataset_name)
    c = dataset.get_column_handle()
    where_value = 1
    having_value = 1000
    stmt = (
        dataset.select(
            func.avg(c.col2).label(col2_mean_label),
            func.toYear(c.col3).label(col3_year_label),
        )
        .where(func.toMonth(c.col3) == where_value)
        .group_by(func.toYear(c.col3))
        .having(func.avg(c.col2) > having_value)
    )
    query = resource.compile(stmt)
    actual = remove_new_lines(query.sql)
    assert actual == expected
    assert query.parameters[having_placeholder] == having_value
    assert query.parameters[where_placeholder] == where_value
