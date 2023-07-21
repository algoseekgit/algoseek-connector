"""Tools to fetch S3 dataset metadata from the metadata-services API."""

from functools import lru_cache
from typing import Any

from ..base import InvalidDataSetName
from ..metadata_api import BaseAPIConsumer


class S3DatasetMetadataAPIConsumer(BaseAPIConsumer):
    """Fetch metadata from S3 datasets."""

    @lru_cache
    def _fetch_dataset_metadata(self) -> dict[str, dict]:
        """Fetch metadata from all datasets."""
        endpoint = "public/cloud_storage/"
        response = self.get(endpoint)
        metadata = dict()
        for dataset_metadata in response.json():
            bucket_groups = dataset_metadata["bucket_groups"]
            has_csv_columns = len(dataset_metadata["csv_columns"]) > 0
            has_bucket_groups = isinstance(bucket_groups, list) and len(bucket_groups)
            is_valid_dataset = has_csv_columns and has_bucket_groups
            if is_valid_dataset:
                dataset_name = dataset_metadata["text_id"]
                metadata[dataset_name] = dataset_metadata
        return metadata

    @lru_cache
    def _fetch_bucket_group_metadata(self) -> dict[int, dict]:
        """Fetch metadata from all bucket groups."""
        endpoint = "public/bucket_group/"
        response = self.get(endpoint)
        return {x["id"]: x for x in response.json()}

    def list_datasets(self) -> list[str]:
        """List the name of all datasets."""
        return list(self._fetch_dataset_metadata())

    def get_dataset_metadata(self, name: str) -> dict[str, Any]:
        """
        Get a dataset metadata.

        Parameters
        ----------
        name : str
            The dataset name

        Returns
        -------
        dict
            A dictionary with metadata. See
            https://metadata-services.algoseek.com/docs for
            `cloud_storage/text_id/{text_id}` for the expected format.

        Raises
        ------
        InvalidDatasetName
            If an non-existent dataset name is passed.

        """
        try:
            metadata = self._fetch_dataset_metadata()
            return metadata[name]
        except KeyError:
            raise InvalidDataSetName(name) from None

    def get_bucket_group_metadata(self, dataset: str) -> dict[str, Any]:
        """
        Get the metadata from a primary bucket group.

        Parameters
        ----------
        dataset : str
            The dataset name.

        Returns
        -------
        dict

        A dictionary with metadata. See
            https://metadata-services.algoseek.com/docs for
            `bucket_group/text_id/{text_id}` for the expected format.

        Raises
        ------
        ValueError
            If no primary bucket group is found for the dataset.

        """
        dataset_metadata = self.get_dataset_metadata(dataset)
        primary_bucket_group = None
        for bucket_group in dataset_metadata["bucket_groups"]:
            bucket_group_metadata = self._fetch_bucket_group_metadata()[bucket_group]
            if bucket_group_metadata["is_primary"]:
                primary_bucket_group = bucket_group_metadata

        if primary_bucket_group is None:
            msg = f"No primary bucket group found for dataset {dataset}."
            raise ValueError(msg)

        return primary_bucket_group

    def list_bucket_groups(self) -> list[int]:
        """Get a list of all available bucket groups."""
        return list(self._fetch_bucket_group_metadata())
