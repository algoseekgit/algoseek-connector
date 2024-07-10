import os
from pathlib import Path

import boto3
import pytest
from algoseek_connector.s3 import downloader
from algoseek_connector.settings import AlgoseekConnectorSettings
from botocore.exceptions import ClientError

DEV_BUCKET = "algoseek-connector-dev"


@pytest.fixture(scope="module")
def dev_session():
    user = os.getenv("ALGOSEEK__DEV__AWS_ACCESS_KEY_ID")
    password = os.getenv("ALGOSEEK__DEV__AWS_SECRET_ACCESS_KEY")
    return downloader.create_boto3_session(aws_access_key_id=user, aws_secret_access_key=password)


@pytest.fixture(scope="module")
def dataset_session():
    s3_config = AlgoseekConnectorSettings().s3
    secret = None if s3_config.aws_secret_access_key is None else s3_config.aws_secret_access_key.get_secret_value()
    return downloader.create_boto3_session(aws_access_key_id=s3_config.aws_access_key_id, aws_secret_access_key=secret)


def test_create_boto3_session_invalid_aws_access_key_id(monkeypatch):
    aws_access_key_id = "InvalidKeyId"
    with pytest.raises(ClientError):
        downloader.create_boto3_session(aws_access_key_id=aws_access_key_id)


def test_create_boto3_session_invalid_aws_secret_access_key():
    aws_secret_access_key = "InvalidSecretKey"
    with pytest.raises(TypeError):
        downloader.create_boto3_session(aws_secret_access_key=aws_secret_access_key)


def test_create_boto3_session(dataset_session: boto3.Session):
    credentials = dataset_session.get_credentials()
    session = downloader.create_boto3_session(
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )
    assert session.profile_name == "default"


def test_BucketWrapper(dev_session: boto3.Session):
    s3 = downloader.get_s3_client(dev_session)
    downloader.BucketWrapper(s3, DEV_BUCKET)
    assert True


def test_get_bucket_non_existent_bucket(dev_session: boto3.Session):
    s3 = downloader.get_s3_client(dev_session)
    with pytest.raises(ValueError):
        bucket_name = "InvalidBucketName"
        downloader.BucketWrapper(s3, bucket_name)


def test_BucketWrapper_check_exists_object_false(dev_session: boto3.Session):
    s3 = downloader.get_s3_client(dev_session)
    bucket = downloader.BucketWrapper(s3, DEV_BUCKET)
    key = "NonExistentObjectKey"
    assert not bucket.check_object_exists(key)


def test_BucketWrapper_upload_file(dev_session: boto3.Session, tmp_path: Path):
    s3 = downloader.get_s3_client(dev_session)
    bucket = downloader.BucketWrapper(s3, DEV_BUCKET)
    key = "test-file.txt"

    file_path = tmp_path / key
    with open(file_path, "wt") as f:
        f.write("Hello, World!\n")

    # upload file and check that it exists.
    assert not bucket.check_object_exists(key)
    bucket.upload_file(key, file_path)
    assert bucket.check_object_exists(key)

    # delete file
    bucket.delete_file(key)
    assert not bucket.check_object_exists(key)


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
