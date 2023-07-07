import pytest

from algoseek_connector import ResourceManager, base
from algoseek_connector.base import DataSource


@pytest.fixture(scope="module")
def data_source():
    # Connect to DB using host, user and password from env variables.
    manager = ResourceManager()
    return manager.create_data_source("clickhouse")


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
            assert dataset_name == dataset.name


def test_ClickHouseClient_get_dataset_invalid_dataset_name(
    data_source: DataSource,
):
    group_name = data_source.list_datagroups()[0]
    group = data_source.fetch_datagroup(group_name)
    with pytest.raises(base.InvalidDataSetName):
        dataset_name = "InvalidDatasetName"
        group.fetch_dataset(dataset_name)


def test_ClickHouseClient_fetch(data_source: DataSource):
    group_name = "USEquityReferenceData"
    group = data_source.fetch_datagroup(group_name)
    print(group.datasets.ASIDLookupBase._dataset)
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
    group = data_source.fetch_datagroup(group_name)
    dataset_name = group.list_datasets()[0]
    dataset = group.fetch_dataset(dataset_name)
    # the first chunk contains headers. make all chunks with size=10
    # except the first one
    limit = 8192 * 2
    chunk_size = 8192
    stmt = dataset.select().limit(limit)
    for i, chunk in enumerate(dataset.fetch_iter(stmt, size=chunk_size)):
        for col_name, v in chunk.items():
            column = dataset[col_name]
            assert col_name == column.name
            assert len(v) == chunk_size
            if hasattr(column.type, "nested_type"):
                assert isinstance(v[0], column.type.nested_type.python_type)
            else:
                assert isinstance(v[0], column.type.python_type)
