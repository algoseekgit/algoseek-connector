import pytest

from algoseek_connector import base
from algoseek_connector.base import DataSource
from algoseek_connector.clickhouse.client import ClickHouseClient


@pytest.fixture(scope="module")
def data_source():
    # Connect to DB using host, user and password from env variables.
    return base.DataSource(ClickHouseClient())


def test_ClickHouseClient_list_groups(data_source: DataSource):
    groups = data_source.list_datagroups()
    assert len(groups)
    assert all(isinstance(x, str) for x in groups)


def test_ClickHouseClient_list_dataset(data_source: DataSource):
    for group in data_source.groups.values():
        for dataset_name in group.list_datasets():
            assert isinstance(dataset_name, str)


def test_ClickHouseClient_get_datagroup_invalid_group(data_source: DataSource):
    with pytest.raises(base.InvalidDataGroupName):
        group = "InvalidGroupName"
        data_source.get_datagroup(group)


def test_ClickHouseClient_get_dataset(data_source: DataSource):
    for group_name in data_source.groups:
        group = data_source.groups[group_name]
        for dataset_name in group.list_datasets():
            dataset = group.fetch_dataset(dataset_name)
            assert dataset_name == dataset.name


def test_ClickHouseClient_get_dataset_invalid_dataset_name(
    data_source: DataSource,
):
    group_name = data_source.list_datagroups()[0]
    group = data_source.groups[group_name]
    with pytest.raises(base.InvalidDataSetName):
        dataset_name = "InvalidDatasetName"
        group.fetch_dataset(dataset_name)


def test_ClickHouseClient_fetch(data_source: DataSource):
    group_name = "USEquityReferenceData"
    group = data_source.groups[group_name]
    dataset_name = group.list_datasets()[0]
    dataset = group.fetch_dataset(dataset_name)
    size = 10
    stmt = dataset.select().limit(size)
    data = dataset.fetch(stmt)
    for k, v in data.items():
        column = dataset[k]
        assert k == column.name
        assert len(v) == size
        if hasattr(column.type, "nested_type"):
            assert isinstance(v[0], column.type.nested_type.python_type)
        else:
            assert isinstance(v[0], column.type.python_type)


def test_ClickHouseClient_fetch_iter(data_source: DataSource):
    group_name = "USEquityReferenceData"
    group = data_source.groups[group_name]
    dataset_name = group.list_datasets()[0]
    dataset = group.fetch_dataset(dataset_name)
    # the first chunk contains headers. make all chunks with size=10
    # except the first one
    limit = 49
    chunk_size = 10
    stmt = dataset.select().limit(limit)
    for i, chunk in enumerate(dataset.fetch_iter(stmt, size=chunk_size)):
        for col_name, v in chunk.items():
            column = dataset[col_name]
            assert col_name == column.name
            if i == 0:  # first chunk is shorter as it contains headers
                assert len(v) == chunk_size - 1
            else:
                assert len(v) == chunk_size

            if hasattr(column.type, "nested_type"):
                assert isinstance(v[0], column.type.nested_type.python_type)
            else:
                assert isinstance(v[0], column.type.python_type)
