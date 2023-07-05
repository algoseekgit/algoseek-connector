"""Tests data retrieved from the DB."""

import pytest
from clickhouse_sqlalchemy import types as clickhouse_types
from sqlalchemy import func

from algoseek_connector.base import DataSet, DataSource
from algoseek_connector.clickhouse.client import ClickHouseClient


@pytest.fixture(scope="module")
def data_source():
    # Connect to DB using host, user and password from env variables.

    return DataSource(ClickHouseClient())


@pytest.fixture(scope="module")
def dataset(data_source: DataSource):
    group_name = "USEquityMarketData"
    dataset_name = "TradeOnlyAdjustedMinuteBar"
    group = data_source.get_datagroup(group_name)
    return group.fetch_dataset(dataset_name)


def test_select(dataset: DataSet):
    limit = 10
    stmt = dataset.select().limit(limit)
    result = dataset.fetch(stmt)
    for column, (name, data) in zip(dataset.get_column_handle(), result.items()):
        assert name == column.name
        assert len(data) == limit
        if column.type is clickhouse_types.Enum8:
            assert isinstance(data[0], str)


def test_select_one_column(dataset: DataSet):
    limit = 5
    columns = list(dataset.c)
    stmt = dataset.select(columns[0]).limit(limit)
    result = dataset.fetch(stmt)
    assert len(result) == 1


def test_select_two_columns(dataset: DataSet):
    limit = 5
    columns = list(dataset.c)
    stmt = dataset.select(*columns[:2]).limit(limit)
    result = dataset.fetch(stmt)
    assert len(result) == 2


def test_select_where_filter_year(dataset: DataSet):
    limit = 10
    selected_year = 2015
    stmt = (
        dataset.select(dataset["FirstTradePrice"], dataset["TradeDate"])
        .where(func.toYear(dataset["TradeDate"]) == selected_year)
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d in result["TradeDate"]:
        assert d.year == selected_year
    for v in result.values():
        assert len(v) == limit


def test_select_where_logical_and(dataset: DataSet):
    limit = 10
    selected_year = 2015
    trade_price_filter = 1000
    stmt = (
        dataset.select(dataset["FirstTradePrice"], dataset["TradeDate"])
        .where(
            (func.toYear(dataset["TradeDate"]) == selected_year)
            & (dataset["FirstTradePrice"] > trade_price_filter)
        )
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d in result["TradeDate"]:
        assert d.year == selected_year

    for p in result["FirstTradePrice"]:
        assert p > trade_price_filter

    for v in result.values():
        assert len(v) == limit


def test_select_where_logical_or(dataset: DataSet):
    limit = 10
    selected_year = 2015
    trade_price_filter = 1000
    stmt = (
        dataset.select(dataset["FirstTradePrice"], dataset["TradeDate"])
        .where(
            (func.toYear(dataset["TradeDate"]) == selected_year)
            | (dataset["FirstTradePrice"] > trade_price_filter)
        )
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d, p in zip(result["TradeDate"], result["FirstTradePrice"]):
        assert (d.year == selected_year) or (p > trade_price_filter)

    for v in result.values():
        assert len(v) == limit


def test_select_where_in(dataset: DataSet):
    limit = 10
    selected_years = [2015, 2016, 2018]
    stmt = (
        dataset.select(dataset["FirstTradePrice"], dataset["TradeDate"])
        .where(func.toYear(dataset["TradeDate"]).in_(selected_years))
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d in result["TradeDate"]:
        assert d.year in selected_years


def test_select_where_between(dataset: DataSet):
    limit = 10
    price_min = 1000
    price_max = 2000
    stmt = (
        dataset.select(dataset["FirstTradePrice"], dataset["TradeDate"])
        .where(dataset["FirstTradePrice"].between(price_min, price_max))
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for p in result["FirstTradePrice"]:
        assert price_min <= p <= price_max


def test_select_groupby_filter_year(dataset: DataSet):
    agg_year_label = "year"
    year_having_filter = 2015
    stmt = (
        dataset.select(
            func.avg(dataset["FirstTradePrice"]),
            func.toYear(dataset["TradeDate"]).label(agg_year_label),
        )
        .group_by(func.toYear(dataset["TradeDate"]))
        .having(func.toYear(dataset["TradeDate"]) == year_having_filter)
    )
    result = dataset.fetch(stmt)

    for d in result[agg_year_label]:
        assert d == year_having_filter

    for v in result.values():
        assert len(v) == 1
