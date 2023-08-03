"""Tools to fetch S3 dataset metadata from the metadata-services API."""

from functools import lru_cache
from typing import Any

from ..metadata_api import BaseAPIConsumer

BUCKET_GROUPS = "bucket_groups"
CLOUD_STORAGE = "cloud_storage"


class S3DatasetMetadataAPIConsumer(BaseAPIConsumer):
    """Fetch metadata from S3 datasets."""

    @lru_cache
    def _fetch_dataset_metadata(self) -> dict[str, dict]:
        """Fetch metadata from all datasets."""
        all_metadata = super()._fetch_dataset_metadata()
        s3_metadata = dict()
        for dataset_metadata in all_metadata.values():
            cloud_source_metadata = dataset_metadata[CLOUD_STORAGE]
            if cloud_source_metadata is None:
                continue
            bucket_groups = cloud_source_metadata[BUCKET_GROUPS]
            has_csv_columns = len(cloud_source_metadata["csv_columns"]) > 0
            has_bucket_groups = isinstance(bucket_groups, list) and len(bucket_groups)
            is_valid_dataset = has_csv_columns and has_bucket_groups
            if is_valid_dataset:
                text_id = dataset_metadata["text_id"]
                s3_metadata[text_id] = dataset_metadata
        return s3_metadata

    @lru_cache
    def get_bucket_group(self, id_: int) -> dict[str, Any]:
        """
        Fetch metadata from a bucket group.

        Parameters
        ----------
        id_ : int
            The bucket group id.

        Returns
        -------
        dict
            The bucket group metadata. See the documentation at
            https://metadata-services.algoseek.com/docs for the
            `api/v1/public/bucket_group/{bucket_group_id}` for the expected
            format.

        Raises
        ------
        requests.exceptions.HTTPError
            If a non-existent bucket group id is passed.

        """
        endpoint = f"public/bucket_group/{id_}/"
        return self.get(endpoint).json()

    def get_dataset_primary_bucket_group(self, text_id: str) -> dict[str, Any]:
        """
        Get the metadata from the primary bucket group.

        Parameters
        ----------
        text_id : str
            The dataset text id.

        Returns
        -------
        dict
            The bucket group metadata. See the documentation at
            https://metadata-services.algoseek.com/docs for the
            `api/v1/public/bucket_group/{bucket_group_id}` endpoint to see
            the expected format.

        Raises
        ------
        InvalidDatasetName
            If an non-existent dataset name is passed.
        ValueError
            If no primary bucket group is found for the dataset.

        """
        dataset_metadata = self.get_dataset_metadata(text_id)
        cloud_storage = dataset_metadata[CLOUD_STORAGE]
        primary_bucket_group = None
        for bucket_group_id in cloud_storage[BUCKET_GROUPS]:
            bucket_group_metadata = self.get_bucket_group(bucket_group_id)
            if bucket_group_metadata["is_primary"]:
                primary_bucket_group = bucket_group_metadata

        if primary_bucket_group is None:
            msg = f"No primary bucket group found for dataset {text_id}."
            raise ValueError(msg)

        return primary_bucket_group

    def get_dataset_bucket_format(self, text_id: str) -> str:
        """
        Get the bucket name format.

        Parameters
        ----------
        text_id : str
            The dataset text id.

        Returns
        -------
        str
            a template with the format for the bucket name.

        """
        bucket_group_metadata = self.get_dataset_primary_bucket_group(text_id)
        return bucket_group_metadata["bucket_name"]

    def get_dataset_bucket_path_format(self, name: str) -> str:
        """
        Get the bucket path format.

        Parameters
        ----------
        name : str
            The dataset name

        Returns
        -------
        str

        Raises
        ------
        InvalidDatasetName
            If an non-existent dataset name is passed.
        """
        bucket_group_metadata = self.get_dataset_primary_bucket_group(name)
        return bucket_group_metadata["path_format"]
