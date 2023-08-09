import pytest

import algoseek_connector as ac


@pytest.fixture(scope="module")
def manager():
    return ac.ResourceManager()


@pytest.fixture(scope="module")
def ardadb(manager):
    return manager.create_data_source(ac.manager.ARDADB)


def test_list_data_sources(manager: ac.ResourceManager):
    actual = manager.list_data_sources()
    assert ac.manager.ARDADB in actual


def test_ardadb_datasource_fetch_data_group(ardadb: ac.base.DataSource):
    group_name = "USEquityMarketData"
    data_group = ardadb.fetch_datagroup(group_name)
    assert data_group.description.name == group_name


def test_ardadb_datasource_fetch_dataset(ardadb: ac.base.DataSource):
    group_name = "USEquityMarketData"
    dataset_name = "TradeOnlyMinuteBar"
    data_group = ardadb.fetch_datagroup(group_name)
    dataset = data_group.fetch_dataset(dataset_name)
    assert dataset.description.name == dataset_name


def test_ardadb_simple_query(ardadb: ac.base.DataSource):
    group_name = "USEquityMarketData"
    dataset_name = "TradeOnlyMinuteBar"
    data_group = ardadb.fetch_datagroup(group_name)
    dataset = data_group.fetch_dataset(dataset_name)

    limit = 5
    stmt = dataset.select().limit(limit)
    df = dataset.fetch_dataframe(stmt)
    assert df.shape[0] == limit
