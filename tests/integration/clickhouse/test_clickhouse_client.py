import filecmp
import os
from pathlib import Path
from typing import cast

import pytest
from pandas import DataFrame

from algoseek_connector import ResourceManager, base, s3
from algoseek_connector.base import DataSet, DataSource
from algoseek_connector.clickhouse import ArdaDBDescriptionProvider
from algoseek_connector.models import DataSourceType

DEV_BUCKET = "algoseek-connector-dev"
ALGOSEEK_DEV_AWS_ACCESS_KEY_ID = os.getenv("ALGOSEEK__DEV__AWS_ACCESS_KEY_ID")
ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY = os.getenv("ALGOSEEK__DEV__AWS_SECRET_ACCESS_KEY")


@pytest.fixture(scope="module")
def data_source():
    manager = ResourceManager()
    return manager.create_data_source(DataSourceType.ARDADB)


@pytest.fixture(scope="module")
def dataset(data_source):
    group_name = "USEquityMarketData"
    group = data_source.fetch_datagroup(group_name)
    dataset_name = "TradeOnly"
    return group.fetch_dataset(dataset_name)


def test_execute_python_types(dataset: DataSet):
    limit = 10
    col_name = "TradeDate"
    stmt = dataset.select(dataset[col_name]).limit(limit)
    expected = dataset.fetch(stmt)

    group = dataset.group
    table_name = f"{group.description.name}.{dataset.description.name}"
    raw_sql = f"SELECT {col_name} FROM {table_name} LIMIT {limit}"
    actual = dataset.execute(raw_sql)
    assert actual == expected


def test_execute_dataframe(dataset: DataSet):
    limit = 10
    col_name = "TradeDate"
    stmt = dataset.select(dataset[col_name]).limit(limit)
    expected = dataset.fetch_dataframe(stmt)
    group = dataset.group
    table_name = f"{group.description.name}.{dataset.description.name}"

    raw_sql = f"SELECT {col_name} FROM {table_name} LIMIT {limit}"
    actual = cast(DataFrame, dataset.execute(raw_sql, output="dataframe"))
    assert expected.equals(actual)


def test_execute_invalid_output(dataset: DataSet):
    limit = 10
    col_name = "TradeDate"
    group = dataset.group
    table_name = f"{group.description.name}.{dataset.description.name}"
    raw_sql = f"SELECT {col_name} FROM {table_name} LIMIT {limit}"
    with pytest.raises(ValueError):
        output = "invalid-output-format"
        dataset.execute(raw_sql, output=output)


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


def test_ClickHouseClient_store_to_s3_non_existing_bucket_raises_value_error(
    dataset: DataSet,
):
    stmt = dataset.select().limit(5)
    bucket = "InvalidAlgoseekConnectorBucket"
    key = "query-data.csv"
    with pytest.raises(ValueError):
        dataset.store_to_s3(
            stmt,
            bucket,
            key,
            aws_access_key_id=ALGOSEEK_DEV_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY,
        )


def test_ClickHouseClient_store_to_s3(dataset: DataSet, tmp_path: Path):
    stmt = dataset.select().limit(5)
    bucket = DEV_BUCKET
    key = "test-query-data.csv"

    dataset.store_to_s3(
        stmt,
        bucket,
        key,
        aws_access_key_id=ALGOSEEK_DEV_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY,
    )

    # download uploaded data
    boto3_session = s3.create_boto3_session(
        aws_access_key_id=ALGOSEEK_DEV_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY,
    )
    s3_client = s3.downloader.get_s3_client(boto3_session)
    bucket = s3.downloader.BucketWrapper(s3_client, bucket)
    s3_file_download_path = tmp_path / "downloaded-from-s3.csv"
    bucket.download_file(key, s3_file_download_path)

    # execute a raw query and store data into a csv file
    clickhouse_client = dataset.source.client._client
    compiled_query = dataset.compile(stmt)
    raw_stmt_str = compiled_query.sql + "\n FORMAT CSVWithNames"
    csv_str = clickhouse_client.raw_query(raw_stmt_str, compiled_query.parameters)
    expected_file_path = tmp_path / "csv-from-clickhouse.csv"
    with open(expected_file_path, "wb") as f:
        f.write(csv_str)

    # compare csv files from s3 and converted to csv
    assert filecmp.cmp(s3_file_download_path, expected_file_path, shallow=False)

    # delete uploaded file
    bucket.delete_file(key)


def test_ClickHouseClient_store_to_s3_overwrite_raises_error(dataset: DataSet, tmp_path: Path):
    stmt = dataset.select().limit(5)
    bucket = DEV_BUCKET
    key = "test-query-data.csv"

    # store file and try to overwrite
    dataset.store_to_s3(
        stmt,
        bucket,
        key,
        aws_access_key_id=ALGOSEEK_DEV_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY,
    )
    with pytest.raises(ValueError):
        dataset.store_to_s3(
            stmt,
            bucket,
            key,
            aws_access_key_id=ALGOSEEK_DEV_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY,
        )

    # delete stored file
    boto3_session = s3.create_boto3_session(
        aws_access_key_id=ALGOSEEK_DEV_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=ALGOSEEK_DEV_AWS_SECRET_ACCESS_KEY,
    )
    s3_client = s3.downloader.get_s3_client(boto3_session)
    bucket = s3.downloader.BucketWrapper(s3_client, bucket)
    bucket.delete_file(key)


@pytest.mark.parametrize(
    "ardadb_group,api_group",
    [
        ("USEquityMarketData", "us_equity"),
        ("USEquityReferenceData", "us_equity_ref"),
        ("USFuturesMarketData", "us_futures"),
    ],
)
def test_ArdaDBDescriptionProvider_get_api_group_text_id(data_source: DataSource, ardadb_group, api_group):
    description_provider = cast(ArdaDBDescriptionProvider, data_source.description_provider)
    actual = description_provider._get_api_data_group_name(ardadb_group)
    assert actual == api_group


# TODO: remove old test
# @pytest.mark.parametrize(
#     "ardadb_group,ardadb_dataset,dataset_text_id",
#     [
#         ("USEquityMarketData", "TradeAndQuote", "eq_taq"),
#         ("USEquityMarketData", "TradeOnly", "eq_trades"),
#         ("USEquityReferenceData", "BasicAdjustments", "eq_adj_factors_basic"),
#     ],
# )
# def test_ArdaDBDescriptionProvider_get_api_dataset_text_id(
#     data_source: DataSource,
#     ardadb_group: str,
#     ardadb_dataset: str,
#     dataset_text_id: str,
# ):
#     description_provider = cast(ArdaDBDescriptionProvider, data_source.description_provider)
#     actual = description_provider._get_api_dataset_destination_id(ardadb_group, ardadb_dataset)
#     assert actual == dataset_text_id
