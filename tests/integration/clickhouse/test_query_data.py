"""Tests data retrieved from the DB."""

import pytest
from clickhouse_sqlalchemy import types as clickhouse_types
from sqlalchemy import func

from algoseek_connector import ResourceManager
from algoseek_connector.base import DataSet, DataSource
from algoseek_connector.models import DataSourceType


@pytest.fixture(scope="module")
def data_source():
    manager = ResourceManager()
    return manager.create_data_source(DataSourceType.ARDADB)


@pytest.fixture(scope="module")
def dataset(data_source: DataSource):
    group_name = "USEquityReferenceData"
    dataset_name = "BasicAdjustments"
    group = data_source.fetch_datagroup(group_name)
    return group.fetch_dataset(dataset_name)


@pytest.mark.parametrize("n", [5, 10, 20])
def test_head(n, dataset: DataSet):
    df = dataset.head(n)
    assert df.shape[0] == n


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
        dataset.select(dataset["Ticker"], dataset["EffectiveDate"])
        .where(func.toYear(dataset["EffectiveDate"]) == selected_year)
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d in result["EffectiveDate"]:
        assert d.year == selected_year
    for v in result.values():
        assert len(v) == limit


def test_select_where_logical_and(dataset: DataSet):
    limit = 10
    selected_year = 2015
    adjustment_factor_filter = 0.99
    stmt = (
        dataset.select(dataset["AdjustmentFactor"], dataset["EffectiveDate"])
        .where(
            (func.toYear(dataset["EffectiveDate"]) == selected_year)
            & (dataset["AdjustmentFactor"] > adjustment_factor_filter)
        )
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d in result["EffectiveDate"]:
        assert d.year == selected_year

    for p in result["AdjustmentFactor"]:
        assert p > adjustment_factor_filter

    for v in result.values():
        assert len(v) == limit


def test_select_where_logical_or(dataset: DataSet):
    limit = 10
    selected_year = 2015
    adjustment_rate_filter = 0.99
    stmt = (
        dataset.select(dataset["AdjustmentFactor"], dataset["EffectiveDate"])
        .where(
            (func.toYear(dataset["EffectiveDate"]) == selected_year)
            | (dataset["AdjustmentFactor"] > adjustment_rate_filter)
        )
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d, p in zip(result["EffectiveDate"], result["AdjustmentFactor"]):
        assert (d.year == selected_year) or (p > adjustment_rate_filter)

    for v in result.values():
        assert len(v) == limit


def test_select_where_in(dataset: DataSet):
    limit = 10
    selected_years = [2015, 2016, 2018]
    stmt = (
        dataset.select(dataset["AdjustmentFactor"], dataset["EffectiveDate"])
        .where(func.toYear(dataset["EffectiveDate"]).in_(selected_years))
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for d in result["EffectiveDate"]:
        assert d.year in selected_years


def test_select_where_between(dataset: DataSet):
    limit = 10
    min_factor = 0.985
    max_factor = 0.995
    stmt = (
        dataset.select(dataset["AdjustmentFactor"], dataset["EffectiveDate"])
        .where(dataset["AdjustmentFactor"].between(min_factor, max_factor))
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    for p in result["AdjustmentFactor"]:
        assert min_factor <= p <= max_factor


def test_select_groupby_filter_year(dataset: DataSet):
    agg_year_label = "year"
    year_having_filter = 2015
    stmt = (
        dataset.select(
            func.avg(dataset["AdjustmentFactor"]),
            func.toYear(dataset["EffectiveDate"]).label(agg_year_label),
        )
        .group_by(func.toYear(dataset["EffectiveDate"]))
        .having(func.toYear(dataset["EffectiveDate"]) == year_having_filter)
    )
    result = dataset.fetch(stmt)

    for d in result[agg_year_label]:
        assert d == year_having_filter

    for v in result.values():
        assert len(v) == 1


def test_select_order_by(dataset: DataSet):
    limit = 100
    stmt = (
        dataset.select(dataset["AdjustmentFactor"], dataset["EffectiveDate"])
        .order_by(dataset["AdjustmentFactor"])
        .limit(limit)
    )
    result = dataset.fetch(stmt)

    previous = None
    for f in result["AdjustmentFactor"]:
        if previous is not None:
            assert f >= previous
        previous = f
