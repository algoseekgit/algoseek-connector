import hashlib
import os
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError


def sha1_digest(path: Path) -> str:
    """Compute the SHA-1 hexadecimal digest of a file."""
    BUF_SIZE = 64 * 1024
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def is_file_equal(file: Path, other: Path) -> bool:
    """Compare two files using SHA-1 digest of each file."""
    return sha1_digest(file) == sha1_digest(other)


@pytest.fixture(scope="module")
def session():
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    return boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)


@pytest.fixture(scope="module")
def s3_resource(session: boto3.Session):
    return session.resource("s3")


@pytest.fixture(scope="module")
def dev_bucket(s3_resource):
    return s3_resource.Bucket("algoseek-connector-dev")


def test_create_session_with_invalid_user():
    # passing invalid user/key is possible.
    access_key = "InvalidAccessKeyID"
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session = boto3.Session(
        aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    # in order to check credentials, validation code must be executed
    # creating a sts client and running the get_caller_identity method
    # allows to check if the credentials are valid.
    with pytest.raises(ClientError):
        sts_client = session.client("sts")
        sts_client.get_caller_identity()


def test_create_session_with_invalid_secret_access_key():
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = "InvalidSecretKey"
    session = boto3.Session(
        aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    # creating a sts client and running the get_caller_identity method
    # allows to check if the credentials are valid.
    with pytest.raises(ClientError):
        sts_client = session.client("sts")
        sts_client.get_caller_identity()


def test_list_bucket_names(s3_resource):
    buckets = [x.name for x in s3_resource.buckets.all()]
    assert "algoseek-connector-dev" in buckets


def test_bucket_exists(dev_bucket):
    # the fastest way to check if a bucket exists using resources seems to be
    # creating a bucket instance and check if theres is a creation date
    # available.
    assert dev_bucket.creation_date is not None


def test_bucket_not_exists(s3_resource):
    bucket = s3_resource.Bucket("InvalidBucketName")
    assert bucket.creation_date is None


def test_list_objects_in_bucket(dev_bucket):
    object_names = [x.key for x in dev_bucket.objects.all()]
    assert "iris.csv" in object_names


def test_object_not_exists(dev_bucket):
    file_object = dev_bucket.Object("InvalidObjectKey")
    with pytest.raises(ClientError):
        file_object.last_modified


def test_get_object_size(dev_bucket):
    file_object = dev_bucket.Object("iris.csv")
    size = file_object.content_length
    assert isinstance(size, int) and (size > 0)


def test_get_object_size_invalid_object(dev_bucket):
    file_object = dev_bucket.Object("InvalidObject")
    with pytest.raises(ClientError):
        file_object.content_length


def test_download_file_from_bucket(dev_bucket, tmp_path: Path):
    file_object = dev_bucket.Object("iris.csv")
    file_path = tmp_path / "iris.csv"
    is_file_before_download = file_path.exists()
    assert not is_file_before_download
    file_object.download_file(file_path)
    is_file_after_download = file_path.exists()
    assert is_file_after_download

    local_file = Path(__file__).parent / "iris.csv"
    assert is_file_equal(file_path, local_file)


def test_download_non_existent_file_from_bucket(dev_bucket, tmp_path: Path):
    file_object = dev_bucket.Object("InvalidObjectKey")
    file_path = tmp_path / "my-file"
    with pytest.raises(ClientError):
        file_object.download_file(file_path)
