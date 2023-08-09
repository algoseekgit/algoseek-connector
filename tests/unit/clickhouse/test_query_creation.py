from typing import cast

import pytest
import sqlparse
from clickhouse_connect.driver import Client
from sqlalchemy import func

from algoseek_connector import base
from algoseek_connector.base import DataGroup, DataSet, DataSetDescription, DataSource
from algoseek_connector.clickhouse.client import (
    ArdaDBDescriptionProvider,
    ClickHouseClient,
)

sql_format_params = {
    "reindent": True,
    "indent_width": 4,
}


class MockClient(ClickHouseClient):
    def list_datagroups(self):
        return list()

    def list_datasets(self, group: str):
        return list()

    def get_dataset_columns(self, group: str, dataset: str):
        descriptions = [
            base.ColumnDescription("col1", "Float64", ""),
            base.ColumnDescription("col2", "Int64", ""),
            base.ColumnDescription("col3", "DateTime64(3, 'Asia/Istanbul')", ""),
            base.ColumnDescription("col4", "Enum8('A' = 1, 'B' = 2, 'C' = 3)", ""),
            base.ColumnDescription("col5", "String", ""),
        ]
        columns = list()
        for c in descriptions:
            columns.append(self._column_factory(c))
        return columns


@pytest.fixture(scope="module")
def dataset():
    ch_client = cast(Client, None)
    client = MockClient(ch_client)
    description_provider = cast(ArdaDBDescriptionProvider, None)
    data_source = DataSource(client, description_provider)

    group_name = "g"
    group_display_name = group_name
    group_description = base.DataGroupDescription(group_name, "", group_display_name)
    group = DataGroup(data_source, group_description)

    dataset_name = "t"
    columns = list()
    dataset_description = DataSetDescription(dataset_name, group_name, columns)

    return DataSet(group, dataset_description)


def test_select_one_column(dataset: DataSet):
    expected = "SELECT g.t.col1 FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(dataset["col1"])
    query = dataset.compile(stmt)
    actual = query.sql
    assert expected == actual


def test_select_two_columns(dataset: DataSet):
    expected = "SELECT g.t.col1, g.t.col2 FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(dataset["col1"], dataset["col2"])
    query = dataset.compile(stmt)
    actual = query.sql
    assert expected == actual


def test_select_all_columns(dataset: DataSet):
    expected = "SELECT g.t.col1, g.t.col2, g.t.col3, g.t.col4, g.t.col5 FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select()
    query = dataset.compile(stmt)
    actual = query.sql
    assert expected == actual


def test_select_exclude_columns(dataset: DataSet):
    expected = "SELECT g.t.col1, g.t.col4 FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    exclude_columns = (dataset["col2"], dataset["col3"], dataset["col5"])
    stmt = dataset.select(exclude=exclude_columns)
    query = dataset.compile(stmt)
    actual = query.sql
    assert expected == actual


def test_select_exclude_all_columns_raise_value_error(dataset: DataSet):
    with pytest.raises(ValueError):
        all_columns = list(dataset.c)
        dataset.select(exclude=all_columns)


def test_select_groupby(dataset: DataSet):
    expected = "SELECT avg(g.t.col1) AS avg_col1 FROM g.t GROUP BY g.t.col4"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(func.avg(dataset["col1"]).label("avg_col1")).group_by(
        dataset["col4"]
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_groupby_two_columns(dataset: DataSet):
    expected = "SELECT avg(g.t.col1) AS avg_col1, g.t.col4 FROM g.t GROUP BY g.t.col4"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(
        func.avg(dataset["col1"]).label("avg_col1"), dataset["col4"]
    ).group_by(dataset["col4"])
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_where(dataset: DataSet):
    expected = "SELECT g.t.col1, g.t.col3 FROM g.t WHERE g.t.col2 = %(col2_1)s"
    expected = sqlparse.format(expected, **sql_format_params)
    col2_filter = 2
    stmt = dataset.select(dataset["col1"], dataset["col3"]).where(
        dataset["col2"] == col2_filter
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["col2_1"] == col2_filter


def test_select_where_logical_and(dataset: DataSet):
    expected = (
        "SELECT g.t.col1 "
        "FROM g.t "
        "WHERE g.t.col2 = %(col2_1)s AND g.t.col1 >= %(col1_1)s"
    )
    expected = sqlparse.format(expected, **sql_format_params)
    col2_filter = 2
    col1_filter = 5
    stmt = dataset.select(dataset["col1"]).where(
        (dataset["col2"] == col2_filter) & (dataset["col1"] >= col1_filter)
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["col2_1"] == col2_filter
    assert query.parameters["col1_1"] == col1_filter


def test_select_where_logical_or(dataset: DataSet):
    expected = (
        "SELECT g.t.col1 "
        "FROM g.t "
        "WHERE g.t.col2 = %(col2_1)s OR g.t.col1 >= %(col1_1)s"
    )
    expected = sqlparse.format(expected, **sql_format_params)
    col2_filter = 2
    col1_filter = 5
    stmt = dataset.select(dataset["col1"]).where(
        (dataset["col2"] == col2_filter) | (dataset["col1"] >= col1_filter)
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["col2_1"] == col2_filter
    assert query.parameters["col1_1"] == col1_filter


def test_select_where_in(dataset: DataSet):
    expected = "SELECT g.t.col1 FROM g.t WHERE g.t.col2 IN (%(col2_1_1)s, %(col2_1_2)s, %(col2_1_3)s)"
    expected = sqlparse.format(expected, **sql_format_params)
    col2_filter = [1, 2, 3]
    stmt = dataset.select(dataset["col1"]).where(dataset["col2"].in_(col2_filter))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert all(x in col2_filter for x in query.parameters.values())


def test_select_where_between(dataset: DataSet):
    expected = (
        "SELECT g.t.col1 FROM g.t WHERE g.t.col2 BETWEEN %(col2_1)s AND %(col2_2)s"
    )
    expected = sqlparse.format(expected, **sql_format_params)
    low = 10
    high = 20
    stmt = dataset.select(dataset["col1"]).where(dataset["col2"].between(low, high))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["col2_1"] == low
    assert query.parameters["col2_2"] == high


def test_select_where_like(dataset: DataSet):
    expected = "SELECT g.t.col1 FROM g.t WHERE g.t.col5 LIKE %(col5_1)s"
    expected = sqlparse.format(expected, **sql_format_params)
    pattern = "pattern%"
    stmt = dataset.select(dataset["col1"]).where(dataset["col5"].like(pattern))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["col5_1"] == pattern


def test_select_order(dataset: DataSet):
    expected = "SELECT g.t.col1 FROM g.t ORDER BY g.t.col2"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(dataset["col1"]).order_by(dataset["col2"])
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_order_desc(dataset: DataSet):
    expected = "SELECT g.t.col1 FROM g.t ORDER BY g.t.col2 DESC"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(dataset["col1"]).order_by(dataset["col2"].desc())
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_order_multiple(dataset: DataSet):
    expected = "SELECT g.t.col1 FROM g.t ORDER BY g.t.col3, g.t.col2 DESC"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select(dataset["col1"]).order_by(
        dataset["col3"], dataset["col2"].desc()
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_limit(dataset: DataSet):
    # TODO: bug in clickhouse-sqlalchemy extra space in LIMIT. make a PR to fix.
    expected = "SELECT g.t.col1, g.t.col2 FROM g.t ORDER BY g.t.col1  LIMIT %(param_1)s"
    expected = sqlparse.format(expected, **sql_format_params)
    limit = 20
    stmt = (
        dataset.select(dataset["col1"], dataset["col2"])
        .order_by(dataset["col1"])
        .limit(limit)
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["param_1"] == limit


def test_select_arithmetic_add_columns(dataset: DataSet):
    col_sum_name = "col_sum"
    expected = f"SELECT g.t.col1 + g.t.col2 AS {col_sum_name} FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select((dataset["col1"] + dataset["col2"]).label(col_sum_name))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_arithmetic_add_literal(dataset: DataSet):
    lit = 5.0
    lit_placeholder = "col1_1"
    col_label = "col1_sum"
    expected = f"SELECT g.t.col1 + %({lit_placeholder})s AS {col_label} FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select((dataset["col1"] + lit).label(col_label))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters["col1_1"] == lit
    assert query.parameters[lit_placeholder] == lit


def test_select_arithmetic_multiply_columns(dataset: DataSet):
    col_label = "col_prod"
    expected = f"SELECT g.t.col1 * g.t.col2 AS {col_label} FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select((dataset["col1"] * dataset["col2"]).label(col_label))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_arithmetic_multiply_literal(dataset: DataSet):
    col_label = "col_prod"
    lit = 5.0
    lit_placeholder = "col1_1"
    expected = f"SELECT g.t.col1 * %({lit_placeholder})s AS {col_label} FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select((dataset["col1"] * lit).label(col_label))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters[lit_placeholder] == lit


def test_select_arithmetic_divide_columns(dataset: DataSet):
    col_label = "col_div"
    expected = f"SELECT g.t.col1 / g.t.col2 AS {col_label} FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select((dataset["col1"] / dataset["col2"]).label(col_label))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_arithmetic_divide_literal(dataset: DataSet):
    col_label = "col_div"
    lit_placeholder = "col1_1"
    lit = 5.0
    expected = f"SELECT g.t.col1 / %({lit_placeholder})s AS {col_label} FROM g.t"
    expected = sqlparse.format(expected, **sql_format_params)
    stmt = dataset.select((dataset["col1"] / lit).label(col_label))
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected


def test_select_complex_query(dataset: DataSet):
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
    expected = sqlparse.format(expected, **sql_format_params)
    where_value = 1
    having_value = 1000
    stmt = (
        dataset.select(
            func.avg(dataset["col2"]).label(col2_mean_label),
            func.toYear(dataset["col3"]).label(col3_year_label),
        )
        .where(func.toMonth(dataset["col3"]) == where_value)
        .group_by(func.toYear(dataset["col3"]))
        .having(func.avg(dataset["col2"]) > having_value)
    )
    query = dataset.compile(stmt)
    actual = query.sql
    assert actual == expected
    assert query.parameters[having_placeholder] == having_value
    assert query.parameters[where_placeholder] == where_value
