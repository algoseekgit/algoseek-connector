import pytest

from algoseek_connector.base import InvalidDataSetName
from algoseek_connector.s3.metadata_api import S3DatasetMetadataAPIConsumer


@pytest.fixture(scope="module")
def api_consumer():
    return S3DatasetMetadataAPIConsumer()


def test_list_datasets(api_consumer: S3DatasetMetadataAPIConsumer):
    datasets = api_consumer.list_datasets()
    assert all(isinstance(x, str) for x in datasets)


def test_get_dataset_metadata(api_consumer: S3DatasetMetadataAPIConsumer):
    expected_keys = [
        "text_id",
        "sample_data_url",
        "sample_file_description",
        "sample_file_id",
        "dataset_id",
        "id",
        "csv_columns",
        "bucket_groups",
    ]
    for dataset_name in api_consumer.list_datasets():
        dataset_metadata = api_consumer.get_dataset_metadata(dataset_name)
        assert all(x in dataset_metadata for x in expected_keys)


def test_get_dataset_metadata_invalid_name(api_consumer: S3DatasetMetadataAPIConsumer):
    dataset_name = "InvalidName"
    with pytest.raises(InvalidDataSetName):
        api_consumer.get_dataset_metadata(dataset_name)


def test_list_bucket_groups(api_consumer: S3DatasetMetadataAPIConsumer):
    bucket_groups = api_consumer.list_bucket_groups()
    assert all(isinstance(x, int) for x in bucket_groups)


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
    for dataset_name in api_consumer.list_datasets():
        bucket_group = api_consumer.get_bucket_group_metadata(dataset_name)
        assert all(x in bucket_group for x in expected_keys)


def test_get_bucket_group_metadata_invalid_dataset(
    api_consumer: S3DatasetMetadataAPIConsumer,
):
    dataset_name = "InvalidDataset"
    with pytest.raises(InvalidDataSetName):
        api_consumer.get_bucket_group_metadata(dataset_name)
