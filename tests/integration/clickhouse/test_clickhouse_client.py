from typing import cast

import pytest
from pandas import DataFrame

from algoseek_connector import base
from algoseek_connector.base import DataSet, DataSource
from algoseek_connector.clickhouse import ArdaDBDescriptionProvider, ClickHouseClient
from algoseek_connector.clickhouse.client import create_clickhouse_client
from algoseek_connector.metadata_api import AuthToken, BaseAPIConsumer


@pytest.fixture(scope="module")
def data_source():
    token = AuthToken()
    api_consumer = BaseAPIConsumer(token)
    description_provider = ArdaDBDescriptionProvider(api_consumer)
    ch_client = create_clickhouse_client()
    client = ClickHouseClient(ch_client)
    return DataSource(client, description_provider)


@pytest.fixture(scope="module")
def dataset(data_source):
    group_name = "USEquityMarketData"
    group = data_source.fetch_datagroup(group_name)
    dataset_name = "TradeOnly"
    return group.fetch_dataset(dataset_name)


def test_execute_python_types(data_source: DataSource, dataset: DataSet):
    limit = 10
    col_name = "TradeDate"
    stmt = dataset.select(dataset[col_name]).limit(limit)
    expected = dataset.fetch(stmt)

    group = dataset.group
    table_name = f"{group.description.name}.{dataset.description.name}"
    raw_sql = f"SELECT {col_name} FROM {table_name} LIMIT {limit}"
    actual = data_source.execute(raw_sql)
    assert actual == expected


def test_execute_dataframe(data_source: DataSource, dataset: DataSet):
    limit = 10
    col_name = "TradeDate"
    stmt = dataset.select(dataset[col_name]).limit(limit)
    expected = dataset.fetch_dataframe(stmt)
    group = dataset.group
    table_name = f"{group.description.name}.{dataset.description.name}"

    raw_sql = f"SELECT {col_name} FROM {table_name} LIMIT {limit}"
    actual = cast(DataFrame, data_source.execute(raw_sql, output="dataframe"))
    assert expected.equals(actual)


def test_ClickHouseClient_list_groups(data_source: DataSource):
    groups = data_source.list_datagroups()
    assert len(groups)
    assert all(isinstance(x, str) for x in groups)


def test_ClickHouseClient_list_dataset(data_source: DataSource):
    for group_name in data_source.groups:
        group = data_source.fetch_datagroup(group_name)
        for dataset_name in group.list_datasets():
            assert isinstance(dataset_name, str)


def test_ClickHouseClient_get_datagroup_invalid_group(data_source: DataSource):
    with pytest.raises(base.InvalidDataGroupName):
        group = "InvalidGroupName"
        data_source.fetch_datagroup(group)


def test_ClickHouseClient_get_dataset(data_source: DataSource):
    for group_name in data_source.list_datagroups():
        group = data_source.fetch_datagroup(group_name)
        for dataset_name in group.list_datasets():
            dataset = group.fetch_dataset(dataset_name)
            assert dataset_name == dataset.description.name


def test_ClickHouseClient_get_dataset_invalid_dataset_name(
    data_source: DataSource,
):
    group_name = data_source.list_datagroups()[0]
    group = data_source.fetch_datagroup(group_name)
    with pytest.raises(base.InvalidDataSetName):
        dataset_name = "InvalidDatasetName"
        group.fetch_dataset(dataset_name)


def test_ClickHouseClient_fetch(dataset: DataSet):
    size = 10
    stmt = dataset.select().limit(size)
    data = dataset.fetch(stmt)
    for k, v in data.items():
        column = dataset[k]
        assert k == column.name
        assert len(v) == size


def test_ClickHouseClient_fetch_dataframe(dataset: DataSet):
    size = 100
    stmt = dataset.select().limit(size)
    df = dataset.fetch_dataframe(stmt)
    n_cols = len(dataset.c)
    assert df.shape == (size, n_cols)


def test_ClickHouseClient_fetch_iter(dataset: DataSet):
    # the minimum possible chunk size is 8192
    limit = 8192 * 2
    chunk_size = 8192
    stmt = dataset.select().limit(limit)
    for chunk in dataset.fetch_iter(stmt, size=chunk_size):
        for col_name, v in chunk.items():
            column = dataset[col_name]
            assert col_name == column.name
            assert len(v) >= chunk_size


def test_ClickHouseClient_fetch_iter_dataframe(dataset: DataSet):
    limit = 8192 * 4
    chunk_size = 8192  # min chunk size is 8292
    stmt = dataset.select().limit(limit)
    n_cols = len(dataset.c)
    for df in dataset.fetch_iter_dataframe(stmt, chunk_size):
        assert df.shape[1] == n_cols
        assert df.shape[0] >= chunk_size
