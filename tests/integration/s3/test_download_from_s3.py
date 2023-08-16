from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError

from algoseek_connector.s3 import downloader


@pytest.fixture(scope="module")
def dev_session():
    return downloader.create_boto3_session(profile_name="algoseek-dev")


@pytest.fixture(scope="module")
def dataset_session():
    return downloader.create_boto3_session(profile_name="algoseek-datasets")


def test_create_boto3_session_invalid_aws_access_key_id():
    aws_access_key_id = "InvalidKeyId"
    with pytest.raises(ClientError):
        downloader.create_boto3_session(aws_access_key_id=aws_access_key_id)


def test_create_boto3_session_invalid_aws_secret_access_key():
    aws_secret_access_key = "InvalidSecretKey"
    with pytest.raises(ClientError):
        downloader.create_boto3_session(aws_secret_access_key=aws_secret_access_key)


def test_create_boto3_session(dataset_session: boto3.Session):
    credentials = dataset_session.get_credentials()
    session = downloader.create_boto3_session(
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )
    assert session.profile_name == "default"


def test_create_boto3_session_using_profile():
    profile_name = "algoseek-datasets"
    session = downloader.create_boto3_session(profile_name=profile_name)
    assert session.profile_name == profile_name


def test_BucketWrapper(dev_session: boto3.Session):
    s3 = downloader._get_s3_client(dev_session)
    downloader.BucketWrapper(s3, "algoseek-connector-dev")
    assert True


def test_get_bucket_non_existent_bucket(dev_session: boto3.Session):
    s3 = downloader._get_s3_client(dev_session)
    with pytest.raises(ValueError):
        bucket_name = "InvalidBucketName"
        downloader.BucketWrapper(s3, bucket_name)


def test_FileDownloader_download(dataset_session, tmp_path: Path):
    file_downloader = downloader.FileDownloader(dataset_session)
    bucket_name = "us-equity-1min-taq-2022"
    path_format = "yyyymmdd/s/sss.csv.gz"

    symbols = ["AAPL", "TAGG"]
    dates = ("20220303", "20220308")
    filters = downloader.S3KeyFilter(symbols=symbols, date=dates)

    keys = list(downloader._generate_object_keys(path_format, filters))
    file_downloader.download(bucket_name, keys, tmp_path)


def test_FileDownloader_copy_session_created_using_profile(
    dataset_session: boto3.Session,
):
    file_downloader = downloader.FileDownloader(dataset_session)
    copy = file_downloader.copy()
    original_credentials = dataset_session.get_credentials()
    copy_credentials = copy.session.get_credentials()

    assert file_downloader.session.profile_name == copy.session.profile_name
    assert copy_credentials.access_key == original_credentials.access_key
    assert copy_credentials.secret_key == original_credentials.secret_key


def test_FileDownloader_copy(dataset_session: boto3.Session):
    credentials = dataset_session.get_credentials()
    session = downloader.create_boto3_session(
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )
    file_downloader = downloader.FileDownloader(session)
    copy = file_downloader.copy()
    copy_credentials = copy.session.get_credentials()

    assert file_downloader.session.profile_name == copy.session.profile_name
    assert copy_credentials.access_key == credentials.access_key
    assert copy_credentials.secret_key == credentials.secret_key
