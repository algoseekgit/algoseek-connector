import pytest

from algoseek_connector.metadata_api import AuthToken, BaseAPIConsumer
from algoseek_connector.s3.client import BucketMetadataProvider


@pytest.fixture(scope="module")
def bucket_metadata():
    token = AuthToken()
    api = BaseAPIConsumer(token)
    return BucketMetadataProvider(api)


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
