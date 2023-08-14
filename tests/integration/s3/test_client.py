from pathlib import Path

import pytest
from boto3 import Session

import algoseek_connector as ac
from algoseek_connector.metadata_api import AuthToken, BaseAPIConsumer
from algoseek_connector.s3.client import (
    BucketMetadataProvider,
    S3DatasetDownloader,
    S3DescriptionProvider,
)
from algoseek_connector.s3.downloader import FileDownloader, create_boto3_session


@pytest.fixture(scope="module")
def api():
    token = AuthToken()
    return BaseAPIConsumer(token)


@pytest.fixture(scope="module")
def bucket_metadata(api):
    return BucketMetadataProvider(api)


@pytest.fixture(scope="module")
def boto3_session():
    return create_boto3_session(profile_name="algoseek-datasets")


@pytest.fixture(scope="module")
def dataset_downloader(bucket_metadata: BucketMetadataProvider, boto3_session: Session):
    downloader = FileDownloader(boto3_session)
    return S3DatasetDownloader(downloader, bucket_metadata)


def test_get_bucket_format(bucket_metadata: BucketMetadataProvider):
    dataset_name = "eq_taq_1min"
    expected = "us-equity-1min-taq-yyyy"
    actual = bucket_metadata.get_dataset_bucket_format(dataset_name)
    assert actual == expected


def test_get_bucket_path_format(bucket_metadata: BucketMetadataProvider):
    dataset_name = "eq_taq_1min"
    expected = "yyyymmdd/s/sss.csv.gz"
    actual = bucket_metadata.get_dataset_bucket_path_format(dataset_name)
    assert actual == expected


def test_S3DatasetDownloader_download_invalid_download_path(
    dataset_downloader: S3DatasetDownloader, tmp_path: Path
):
    date = "20230703"
    symbols = "AAPL"
    download_path = tmp_path / "non-existent-path"
    dataset_text_id = "eq_taq_1min"
    with pytest.raises(NotADirectoryError):
        dataset_downloader.download(dataset_text_id, download_path, date, symbols)


def test_S3DatasetDownloader_download_equity_data(
    dataset_downloader: S3DatasetDownloader, tmp_path: Path
):
    date = ("20230701", "20230705")
    symbols = ["AMZN", "AAPL"]
    download_path = tmp_path / "data"
    download_path.mkdir()
    dataset_text_id = "eq_taq_1min"
    dataset_downloader.download(dataset_text_id, download_path, date, symbols)

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


def test_S3DatasetDownloader_download_invalid_ticker_does_not_raise_exceptions(
    dataset_downloader: S3DatasetDownloader, tmp_path: Path
):
    date = ("20230701", "20230705")
    symbols = ["INVALID-SYMBOL"]
    download_path = tmp_path / "data"
    download_path.mkdir()
    dataset_text_id = "eq_taq_1min"

    dataset_downloader.download(dataset_text_id, download_path, date, symbols)
    assert not list(download_path.glob("*"))  # check that no files were downloaded


def test_S3DatasetDownloader_download_exceed_max_download_size_raises_error(
    dataset_downloader: S3DatasetDownloader, tmp_path: Path, monkeypatch
):
    date = ("20230701", "20230705")
    symbols = ["AMZN", "AAPL"]
    download_path = tmp_path / "data"
    download_path.mkdir()
    dataset_text_id = "eq_taq_1min"

    monkeypatch.setattr(ac.s3.client, "MAX_DOWNLOAD_SIZE", 10)
    with pytest.raises(ac.s3.client.DownloadLimitExceededError):
        dataset_downloader.download(dataset_text_id, download_path, date, symbols)


def test_S3DatasetDownloader_download_from_multiple_years_raises_error(
    dataset_downloader: S3DatasetDownloader, tmp_path: Path
):
    date = ("20220701", "20230705")
    symbols = ["AMZN", "AAPL"]
    download_path = tmp_path / "data"
    download_path.mkdir()
    dataset_text_id = "eq_taq_1min"

    with pytest.raises(ValueError):
        dataset_downloader.download(dataset_text_id, download_path, date, symbols)


@pytest.fixture(scope="module")
def description_provider(api: BaseAPIConsumer):
    return S3DescriptionProvider(api)


def test_S3DescriptionProvider_get_columns_description(
    description_provider: S3DescriptionProvider,
):
    text_id = "eq_taq_1min"
    actual = description_provider.get_columns_description(text_id)
    assert len(actual)
    assert all(isinstance(x, ac.base.ColumnDescription) for x in actual)


def test_S3DescriptionProvider_get_columns_description_non_existent_returns_empty_list(
    description_provider: S3DescriptionProvider,
):
    text_id = "NonExistentDataSet"
    actual = description_provider.get_columns_description(text_id)
    assert not actual


def test_S3DescriptionProvider_get_dataset_description(
    description_provider: S3DescriptionProvider,
):
    dataset_text_id = "eq_taq_1min"
    group_text_id = "us_equity"
    actual = description_provider.get_dataset_description(
        group_text_id, dataset_text_id
    )
    expected_name = dataset_text_id
    assert actual.name == expected_name
    assert actual.display_name != expected_name
    assert actual.description
    assert actual.columns
    assert all(isinstance(x, ac.base.ColumnDescription) for x in actual.columns)


def test_S3DescriptionProvider_get_dataset_description_non_existent_does_not_fail(
    description_provider: S3DescriptionProvider,
):
    dataset_text_id = "NonExistentDataSet"
    group_text_id = "NonExistentDataGroup"
    actual = description_provider.get_dataset_description(
        group_text_id, dataset_text_id
    )
    expected_name = dataset_text_id
    assert actual.name == expected_name
    assert actual.display_name == expected_name
    assert not actual.description
    assert not actual.columns


def test_S3DescriptionProvider_get_datagroup_description(
    description_provider: S3DescriptionProvider,
):
    text_id = "us_equity"
    actual = description_provider.get_datagroup_description(text_id)
    expected_name = text_id
    assert actual.name == expected_name
    assert actual.display_name != expected_name
    assert actual.description


def test_S3DescriptionProvider_get_datagroup_description_non_existent_group_does_not_fail(
    description_provider: S3DescriptionProvider,
):
    text_id = "NonExistentDataGroup"
    actual = description_provider.get_datagroup_description(text_id)
    expected_name = text_id
    assert actual.name == expected_name
    assert actual.display_name == expected_name
    assert not actual.description
