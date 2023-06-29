import pytest

from algoseek_connector.clickhouse.resources import ClickHouseDataResource


@pytest.fixture(scope="module")
def clickhouse_resource():
    # Connect to DB using host, user and password from env variables.
    return ClickHouseDataResource()


def test_ClickHouseDataResource_list_groups(
    clickhouse_resource: ClickHouseDataResource,
):
    groups = clickhouse_resource.list_groups()
    assert len(groups)
    assert all(isinstance(x, str) for x in groups)


def test_ClickHouseDataResource_list_dataset(
    clickhouse_resource: ClickHouseDataResource,
):
    for group in clickhouse_resource.list_groups():
        for dataset in clickhouse_resource.list_datasets(group):
            assert isinstance(dataset, str)


def test_ClickHouseDataResource_list_dataset_invalid_group(
    clickhouse_resource: ClickHouseDataResource,
):
    with pytest.raises(ValueError):
        group = "InvalidGroupName"
        clickhouse_resource.list_datasets(group)


def test_ClickHouseDataResource_get_dataset(
    clickhouse_resource: ClickHouseDataResource,
):
    for group_name in clickhouse_resource.list_groups():
        for dataset_name in clickhouse_resource.list_datasets(group_name):
            dataset = clickhouse_resource.get_dataset(group_name, dataset_name)
            assert dataset_name == dataset.name


def test_ClickHouseDataResource_get_dataset_invalid_dataset_name(
    clickhouse_resource: ClickHouseDataResource,
):
    with pytest.raises(ValueError):
        group_name = clickhouse_resource.list_groups()[0]
        dataset_name = "InvalidDatasetName"
        clickhouse_resource.get_dataset(group_name, dataset_name)


def test_ClickHouseDataResource_get_dataset_invalid_group_name(
    clickhouse_resource: ClickHouseDataResource,
):
    with pytest.raises(ValueError):
        group_name = "InvalidGroupName"
        dataset_name = "InvalidDatasetName"
        clickhouse_resource.get_dataset(group_name, dataset_name)


def test_ClickHouseDataResource_fetch(clickhouse_resource: ClickHouseDataResource):
    group_name = clickhouse_resource.list_groups()[2]
    dataset_name = clickhouse_resource.list_datasets(group_name)[0]
    dataset = clickhouse_resource.get_dataset(group_name, dataset_name)
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


def test_ClickHouseDataResource_fetch_iter(clickhouse_resource: ClickHouseDataResource):
    group_name = clickhouse_resource.list_groups()[2]
    dataset_name = clickhouse_resource.list_datasets(group_name)[0]
    dataset = clickhouse_resource.get_dataset(group_name, dataset_name)
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
