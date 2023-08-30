from pathlib import Path
from typing import cast

import pytest

import algoseek_connector as ac
import algoseek_connector.constants as c


@pytest.fixture(scope="module")
def manager():
    return ac.ResourceManager()


@pytest.fixture(scope="module")
def ardadb(manager):
    return manager.create_data_source(c.ARDADB)


@pytest.fixture(scope="module")
def s3(manager: ac.ResourceManager):
    return manager.create_data_source(c.S3)


def test_list_data_sources(manager: ac.ResourceManager):
    actual = manager.list_data_sources()
    assert c.ARDADB in actual


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


def test_create_s3_data_source(manager: ac.ResourceManager):
    config = {"profile_name": "algoseek-datasets"}
    s3 = manager.create_data_source(c.S3, **config)
    client = cast(ac.s3.S3DownloaderClient, s3.client)

    assert client._file_downloader.session.profile_name == config["profile_name"]


def test_s3_list_data_groups(s3: ac.base.DataSource):
    groups = s3.list_datagroups()
    assert all(isinstance(x, str) for x in groups)


def test_s3_fetch_data_group(s3: ac.base.DataSource):
    group_name = "us_equity"
    group = s3.fetch_datagroup(group_name)
    assert group.description.name == group_name


def test_s3_list_datasets(s3: ac.base.DataSource):
    group_name = "us_equity"
    group = s3.fetch_datagroup(group_name)
    datasets = group.list_datasets()
    assert all(isinstance(x, str) for x in datasets)


def test_s3_download_data_from_dataset(s3: ac.base.DataSource, tmp_path: Path):
    group_name = "us_equity"
    group = s3.fetch_datagroup(group_name)
    dataset = cast(ac.base.DataSetFetcher, getattr(group.datasets, "eq_taq_1min"))

    date = ("20230701", "20230705")
    symbols = ["AMZN", "AAPL"]
    download_path = tmp_path / "data"
    download_path.mkdir()
    dataset.download(download_path, date, symbols)

    # 20230701 and 20230702 are Saturday and Sunday and 20230704 is a holiday
    expected_file_list = [
        "20230703/A/AMZN.csv.gz",
        "20230703/A/AAPL.csv.gz",
        "20230705/A/AMZN.csv.gz",
        "20230705/A/AAPL.csv.gz",
    ]

    for file in expected_file_list:
        file_path = download_path / file
        assert file_path.exists()


def test_ResourceManager_create_invalid_data_source(manager: ac.ResourceManager):
    with pytest.raises(ValueError):
        manager.create_data_source("InvalidDataSource")
