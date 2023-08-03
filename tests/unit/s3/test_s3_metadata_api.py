import pytest

from algoseek_connector.base import InvalidDataSetName
from algoseek_connector.s3.metadata_api import S3DatasetMetadataAPIConsumer


@pytest.fixture(scope="module")
def api_consumer():
    return S3DatasetMetadataAPIConsumer()


def test_get_bucket_group_metadata(api_consumer: S3DatasetMetadataAPIConsumer):
    expected_keys = [
        "text_id",
        "bucket_name",
        "path_format",
        "path_description",
        "aggregation",
        "start_year",
        "end_year",
        "indexed",
        "is_primary",
        "bucket_size_human",
        "total_size_human",
        "bucket_num_objects",
        "bucket_num_objects_human",
        "creation_date",
        "supplementary_bucket_group",
        "cloud_storage_id",
        "status_id",
        "updates_id",
        "sample_data_id",
        "storage_type_id",
        "id",
    ]
    for dataset_name in api_consumer.list_datasets()[:5]:
        bucket_group = api_consumer.get_dataset_primary_bucket_group(dataset_name)
        assert all(x in bucket_group for x in expected_keys)


def test_get_bucket_group_metadata_invalid_dataset(
    api_consumer: S3DatasetMetadataAPIConsumer,
):
    dataset_name = "InvalidDataset"
    with pytest.raises(InvalidDataSetName):
        api_consumer.get_dataset_primary_bucket_group(dataset_name)


def test_get_bucket_format(api_consumer: S3DatasetMetadataAPIConsumer):
    dataset_name = "eq_taq_1min"
    expected = "us-equity-1min-taq-yyyy"
    actual = api_consumer.get_dataset_bucket_format(dataset_name)
    assert actual == expected


def test_get_bucket_path_format(api_consumer: S3DatasetMetadataAPIConsumer):
    dataset_name = "eq_taq_1min"
    expected = "yyyymmdd/s/sss.csv.gz"
    actual = api_consumer.get_dataset_bucket_path_format(dataset_name)
    assert actual == expected
