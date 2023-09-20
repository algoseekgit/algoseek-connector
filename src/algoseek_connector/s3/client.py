"""Client protocol for S3 data."""

import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, Union

from boto3 import Session

from .. import base
from ..metadata_api import BaseAPIConsumer
from . import downloader

BUCKET_GROUPS = "bucket_groups"
CLOUD_STORAGE = "cloud_storage"
MAX_DOWNLOAD_SIZE = 1024**12  # maximum total size to download = 1 TiB

date_like = Union[datetime.date, str]


class S3DownloaderClient(base.ClientProtocol):
    """
    ClientProtocol for downloading files from S3.

    Parameters
    ----------
    session : boto3.Session
    api: :py:class:`algoseek_connector.metadata_api.BaseAPIConsumer`

    Methods
    -------
    create_function_handle:
        Not Implemented.
    execute:
        Not Implemented.
    download:
        Download dataset files using filters.
    fetch:
        Not Implemented.
    fetch_iter:
        Not Implemented.
    fetch_dataframe:
        Not Implemented.
    fetch_iter_dataframe:
        Not Implemented.
    list_datagroups:
        List available data groups.
    list_datasets:
        List available datasets.
    get_dataset_columns:
        Not Implemented.
    compile:
        Not Implemented.
    Store_to_s3:
        Not Implemented.

    """

    def __init__(self, session: Session, api: BaseAPIConsumer):
        self.api = api
        self._bucket_metadata = BucketMetadataProvider(api)
        self._file_downloader = downloader.FileDownloader(session)

    def compile(self, stmt):  # pragma: no cover
        """Compile a SQLAlchemy Select statement into a CompiledQuery."""
        raise NotImplementedError

    def download(
        self,
        dataset_text_id: str,
        download_path: Path,
        date: Union[date_like, tuple[date_like, date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Optional[Union[date_like, tuple[date_like, date_like]]] = None,
    ):
        """
        Download data from the dataset.

        Parameters
        ----------
        dataset_text_id : str
            The dataset text id.
        download_path : pathlib.Path
            Path to a directory to download dataset files.
        date : str, datetime.date or tuple
            Download data in this date range. Dates can be passed as a str with
            `yyyymmdd` format or as date objects. If a tuple is passed, it is
            interpreted as a date range and all dates in the closed interval
            between the two dates are generated. I a single date is passed,
            download data from this specific date.
        symbols : str or list[str]
            Download data associated with these symbols.
        expiration date : str, datetime.date or tuple
            Download data with expiration dates in this date range. Dates must
            be passed used the same format used for the `date` parameter.

        """
        # Creates a dataset downloader using an independent copy.
        # boto3 sessions are not thread/multi process safe, so  this allows to
        # download multiple datasets at the same.
        dataset_downloader = S3DatasetDownloader(
            self._file_downloader.copy(), self._bucket_metadata
        )
        dataset_downloader.download(
            dataset_text_id, download_path, date, symbols, expiration_date
        )

    def get_dataset_columns(
        self, group: str, dataset: str
    ) -> base.DataSetDescription:  # pragma: no cover
        """Create a dataset."""
        raise NotImplementedError

    def create_function_handle(self):  # pragma: no cover
        """Create a FunctionHandle instance."""
        raise NotImplementedError

    def fetch(self, query, **kwargs):  # pragma: no cover
        """Fetch a select query."""
        raise NotImplementedError

    def fetch_iter(self, query, size: int, **kwargs):  # pragma: no cover
        """Yield a select query in chunks."""
        raise NotImplementedError

    def fetch_dataframe(self, query, **kwargs):  # pragma: no cover
        """Fetch a select query and output results as a Pandas DataFrame."""
        raise NotImplementedError

    def fetch_iter_dataframe(self, query, size: int, **kwargs):  # pragma: no cover
        """Yield a select query in chunks, using pandas DataFrames."""
        raise NotImplementedError

    def list_datagroups(self) -> list[str]:
        """List available data groups."""
        return self.api.list_datagroups()

    @lru_cache
    def list_datasets(self, group_text_id: str) -> list[str]:
        """List available data groups."""
        # datasets are listed based on the following conditions:
        # Must have at least a bucket group and csv column information
        group_id = self.api.get_datagroup_metadata(group_text_id)["id"]
        datasets = list()
        for text_id in self.api.list_datasets():
            dataset_metadata = self.api.get_dataset_metadata(text_id)
            if dataset_metadata["datagroup_id"] != group_id:
                continue

            cloud_metadata = dataset_metadata[CLOUD_STORAGE]
            if cloud_metadata is None:
                continue

            bucket_groups = cloud_metadata[BUCKET_GROUPS]
            has_csv_columns = len(cloud_metadata["csv_columns"]) > 0
            has_bucket_groups = isinstance(bucket_groups, list) and len(bucket_groups)
            is_valid_dataset = has_csv_columns and has_bucket_groups

            if is_valid_dataset:
                datasets.append(text_id)
        return datasets

    def store_to_s3(
        self,
        query: base.CompiledQuery,
        path: str,
        aws_key_id: str,
        aws_secret_access_key: str,
    ):  # pragma: no cover
        """Download query to S3."""
        raise NotImplementedError

    def execute(
        self, sql: str, parameters: Optional[dict], output: str, **kwargs
    ):  # pragma: no cover
        """Execute raw SQL query."""
        raise NotImplementedError


class S3DescriptionProvider(base.DescriptionProvider):
    """Provide description for S3 datasets."""

    def __init__(self, api: BaseAPIConsumer):
        self._api = api

    def get_datagroup_description(self, group: str) -> base.DataGroupDescription:
        """
        Get the description of a data group.

        Parameters
        ----------
        group : str
            The data group name.

        Returns
        -------
        DataGroupDescription

        """
        try:
            datagroup_metadata = self._api.get_datagroup_metadata(group)
            display_name = datagroup_metadata["display_name"]
            description = datagroup_metadata["description"]
        except base.InvalidDataGroupName:
            description = ""
            display_name = group
        return base.DataGroupDescription(group, description, display_name)

    def get_columns_description(self, dataset: str) -> list[base.ColumnDescription]:
        """
        Get the description of the dataset columns.

        Parameters
        ----------
        dataset : str
            The dataset name.

        Returns
        -------
        list[ColumnDescription]

        """
        try:
            dataset_metadata = self._api.get_dataset_metadata(dataset)
            cloud_metadata = dataset_metadata[CLOUD_STORAGE]
            columns = list()
            for column in cloud_metadata["csv_columns"]:
                c = base.ColumnDescription(
                    column["name"], column["data_type"], column["description"]
                )
                columns.append(c)
        except base.InvalidDataSetName:
            columns = list()
        return columns

    def get_dataset_description(
        self, group: str, dataset: str
    ) -> base.DataSetDescription:
        """
        Get the description of a dataset.

        group : str
            The datagroup name.
        dataset : str
            The dataset name.

        Returns
        -------
        DatasetDescription

        Raises
        ------
        InvalidDataSetName

        """
        columns = self.get_columns_description(dataset)
        try:
            dataset_metadata = self._api.get_dataset_metadata(dataset)
            display_name = dataset_metadata["display_name"]
            description = dataset_metadata["long_description"]
            # get platform metadata if available
            # search platform metadata if available
            try:
                platform_metadata = self._api.get_platform_dataset_metadata(dataset)
                pdf_url = platform_metadata["documentation_link"]
                sample_data_url = platform_metadata["sample_data_url"]
            except ValueError:
                pdf_url = None
                sample_data_url = None

            granularity_id = dataset_metadata["time_granularity_id"]
            granularity_metadata = self._api.get_time_granularity_metadata(
                granularity_id
            )
            granularity = granularity_metadata["display_name"]

        except base.InvalidDataSetName:
            display_name = dataset
            description = ""
            pdf_url = None
            sample_data_url = None
            granularity = None
        return base.DataSetDescription(
            dataset,
            group,
            columns,
            display_name,
            description,
            granularity,
            pdf_url,
            sample_data_url,
        )


class BucketMetadataProvider:
    """Fetch metadata from S3 datasets."""

    def __init__(self, api: BaseAPIConsumer) -> None:
        self.api = api

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
        return self.api.get(endpoint).json()

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
        dataset_metadata = self.api.get_dataset_metadata(text_id)
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


class S3DatasetDownloader:
    """Interface to download datasets stored on S3 buckets."""

    def __init__(
        self,
        file_downloader: downloader.FileDownloader,
        bucket_data_provider: BucketMetadataProvider,
    ):
        self.bucket_metadata = bucket_data_provider
        self.downloader = file_downloader

    def download(
        self,
        text_id: str,
        download_path: Union[str, Path],
        date: Union[date_like, tuple[date_like, date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Union[date_like, tuple[date_like, date_like], None] = None,
    ):
        """
        Download data from the dataset.

        Parameters
        ----------
        text_id : str
            The dataset text id.
        download_path : str or pathlib.Path
            Path to a directory to download dataset files.
        date : str, datetime.date or tuple
            Download data in this date range. Dates can be passed as a str with
            `yyyymmdd` format or as date objects. If a tuple is passed, it is
            interpreted as a date range and all dates in the closed interval
            between the two dates are generated. I a single date is passed,
            download data from this specific date.
        symbols : str or list[str]
            Download data associated with these symbols.
        expiration date : str, datetime.date or tuple
            Download data with expiration dates in this date range. Dates must
            be passed used the same format used for the `date` parameter.

        """
        if isinstance(download_path, str):
            download_path = Path(download_path)

        if not download_path.is_dir():
            msg = f"{download_path} is not a directory."
            raise NotADirectoryError(msg)

        filters = downloader.S3KeyFilter(date, symbols, expiration_date)
        bucket_format = self.bucket_metadata.get_dataset_bucket_format(text_id)
        bucket_name = downloader.get_bucket_name(bucket_format, *filters.date)
        bucket = downloader.BucketWrapper(self.downloader.s3, bucket_name)

        path_format = self.bucket_metadata.get_dataset_bucket_path_format(text_id)
        key_to_size = downloader.create_key_to_size_dictionary(
            bucket, path_format, filters
        )
        total_size = sum(key_to_size.values())
        if total_size > MAX_DOWNLOAD_SIZE:
            msg = (
                f"The total size of the requested data is {total_size}. "
                f"Maximum allowed is {MAX_DOWNLOAD_SIZE}."
            )
            raise DownloadLimitExceededError(msg)

        # TODO: multi process download.
        keys = list(key_to_size)
        self.downloader.download(bucket_name, keys, download_path)


class DownloadLimitExceededError(ValueError):
    """Exception raised when trying to download files above the maximum allowed."""

    pass
