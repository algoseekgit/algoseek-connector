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


def test_BucketWrapper(dev_session: boto3.Session):
    s3 = downloader._get_s3_client(dev_session)
    downloader.BucketWrapper(s3, "algoseek-connector-dev")
    assert True


def test_get_bucket_non_existent_bucket(dev_session: boto3.Session):
    s3 = downloader._get_s3_client(dev_session)
    with pytest.raises(ValueError):
        bucket_name = "InvalidBucketName"
        downloader.BucketWrapper(s3, bucket_name)


def test_download_files_from_bucket(dataset_session, tmp_path: Path):
    file_downloader = downloader.FileDownloader(dataset_session)
    bucket_name = "us-equity-1min-taq-2022"
    path_format = "yyyymmdd/s/sss.csv.gz"

    symbols = ["AAPL", "TAGG"]
    dates = ("20220303", "20220308")
    filters = downloader.S3KeyFilter(symbols=symbols, date=dates)

    keys = list(downloader._generate_object_keys(path_format, filters))
    file_downloader.download(bucket_name, keys, tmp_path)
