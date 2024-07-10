"""New API version."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

import pendulum
import pydantic
import requests
import requests.auth

from .base import InvalidDataGroupName, InvalidDataSetName
from .models import DatasetAPIConfiguration, DataSourceType
from .settings import load_settings

logger = logging.getLogger(__file__)


class DatasetAPIProvider:
    """
    Provide access to metadata API v2.

    By default, a timeout of 5 s is set to all requests.

    Parameters
    ----------
    token : AuthToken
        The API authentication token.

    Methods
    -------
    get:
        Request data from an endpoint using the GET method.
    list_datagroups:
        List available data groups.
    list_datasets:
        List available datasets.
    get_dataset:
        Get the metadata of a dataset.
    get_data_group:
        Get the metadata of a data group.
    get_dataset_details:
        Get extended information of a dataset.
    get_dataset_name:
        Get the dataset name used when creating dataset instances.
    get_dataset_destination_id:
        Get a dataset destination id using a dataset name.

    """

    def __init__(self, config: DatasetAPIConfiguration | None = None):
        # TODO: temp workaround. Fix when pydantic based settings is implemented
        if config is None:
            config = load_settings().dataset_api

        self.config = config
        self.session = requests.Session()
        self.session.auth = BearerAuth(config)
        self.session.headers = {"timeout": "5.0", "accept": "application/json"}
        if config.headers is not None:
            self.session.headers.update(config.headers)

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Request data using the GET method.

        Parameters
        ----------
        endpoint : str
            The endpoint to request data from
        kwargs : dict
            Keyword arguments passed to :py:meth:`requests.Session.get`

        """
        url = f"{self.config.url}/{endpoint}"
        logger.info(f"Performing GET request to {url}...")
        response = self.session.get(url, **kwargs)
        if response.status_code != requests.codes.OK:
            msg = (
                f"GET request to Dataset API endpoint {endpoint} failed with code {response.status_code}: \n"
                f"{response.json()}"
            )
            raise requests.exceptions.HTTPError(msg)
        return response

    @lru_cache
    def _fetch_datagroups(self) -> dict[str, DataGroupApiInfo]:
        endpoint = "extras/algoseek-connector/data-groups"
        data = dict()
        for d in self.get(endpoint).json():
            info = DataGroupApiInfo(**d)
            data[info.internal_name] = info
        return data

    @lru_cache
    def _fetch_datasets(self) -> dict[int, DatasetVersionApiInfo]:
        endpoint = "extras/algoseek-connector/destinations"
        data = dict()
        for d in self.get(endpoint).json():
            info = DatasetVersionApiInfo(**d)
            data[info.destination_id] = info
        return data

    @lru_cache
    def _dataset_text_id_to_dataset_destination_id(self) -> dict[str, int]:
        return {v.dataset_text_id: k for k, v in self._fetch_datasets().items()}

    def list_data_groups(self) -> list[str]:
        """List all available data groups."""
        return list(self._fetch_datagroups())

    def list_dataset_destinations(self) -> list[int]:
        """List all available dataset destinations."""
        return list(self._fetch_datasets())

    def get_data_group(self, internal_name: str) -> DataGroupApiInfo:
        """Retrieve data group information.

        Parameters
        ----------
        internal_name : str
            The data group internal name as registered in the dataset API.

        """
        try:
            return self._fetch_datagroups()[internal_name]
        except KeyError as e:
            msg = f"Requested data group `{internal_name}` not found in dataset API."
            raise InvalidDataGroupName(msg) from e

    def get_dataset(self, destination_id: int) -> DatasetVersionApiInfo:
        """Retrieve dataset destination information.

        Parameters
        ----------
        destination_id : int
            The dataset destination id as registered in the dataset API.

        """
        try:
            return self._fetch_datasets()[destination_id]
        except KeyError as e:
            msg = f"Requested dataset destination {destination_id} not found in dataset API."
            raise InvalidDataSetName(msg) from e

    @lru_cache
    def get_dataset_details(self, destination_id: int) -> DatasetDetails:
        """Retrieve dataset schema and long description.

        Parameters
        ----------
        destination_id : int
            The dataset destination id as registered in the dataset API.

        """
        endpoint = f"extras/algoseek-connector/destinations/{destination_id}"
        return DatasetDetails(**self.get(endpoint).json())

    def get_dataset_name(self, destination_id: int) -> str:
        """Create a unique display name for a dataset."""
        dataset = self.get_dataset(destination_id)
        if dataset.destination_type == DataSourceType.ARDADB and dataset.table_name is not None:
            return dataset.table_name.split(".")[-1]
        elif dataset.destination_type == DataSourceType.S3:
            if dataset.version_number > 1:
                return f"{dataset.dataset_text_id}-v{dataset.version_number}"
            return dataset.dataset_text_id
        else:
            # TODO: replace with a better error type
            raise ValueError(f"Could not find dataset name for dataset with destination id {destination_id}.")

    def get_dataset_destination_id(self, name: str) -> int:
        """Get the dataset destination id from its name."""
        text_id_to_destination_id = self._dataset_text_id_to_dataset_destination_id()
        if name in text_id_to_destination_id:
            return text_id_to_destination_id[name]

        try:
            text_id, _ = name.split("-")
            return text_id_to_destination_id[text_id]
        except (ValueError, KeyError) as e:
            raise InvalidDataSetName(f"Invalid dataset name {name}") from e


class DataGroupApiInfo(pydantic.BaseModel):
    """Store data group information retrieved from the dataset API 2.0."""

    model_config = pydantic.ConfigDict(validate_assignment=True)

    internal_name: str
    """The internal data group name used by Algoseek"""

    display_name: str
    """The pretty print data group name"""

    description: str
    """The data group description."""

    @pydantic.field_validator("description", mode="before")
    @classmethod
    def _validate_no_description(cls, description: str | None) -> str:
        return "" if description is None else description

    @pydantic.model_validator(mode="before")
    @classmethod
    def _validate_no_display_name(cls, data: Any) -> Any:
        if data.get("display_name") is None:
            data["display_name"] = data["internal_name"]
        return data


class DatasetVersionApiInfo(pydantic.BaseModel):
    """Store dataset general information retrieved from the dataset API 2.0."""

    destination_id: int
    """The dataset destination primary key"""

    destination_type: str
    """The dataset destination type"""

    is_primary: bool
    """shows if this destination is primary in case multiple destinations of the
    same type are attached to the dataset version
    """

    version_number: int
    """The dataset major version"""

    dataset_text_id: str
    """A reference to the parent dataset"""

    dataset_display_name: str
    """text representation of the data group"""

    short_description: str
    """The dataset's short description"""

    data_group_name: str
    """the parent Data Group primary key"""

    time_granularity: str
    """The dataset time granularity"""

    documentation_link: str | None = None
    """The URL location where the documentation source file is located"""

    sample_data_url: str | None = None
    """The URL location with sample csv data"""

    schema_name: str | None = None
    """refers to the ArdaDbSchema where the table is defined"""

    table_name: str | None = None
    """the name of the database table in the format schema_name.table_name"""

    bucket_name: str | None = None
    """The name of the bucket containing the location"""

    path_format: str | None = None
    """The object path in the bucket corresponding to the location"""


class DatasetDetails(pydantic.BaseModel):
    """Store dataset destination metadata."""

    destination_id: int
    """The dataset destination primary key"""

    destination_type: str
    """The dataset destination type"""

    long_description: str | None = None
    """Detailed description of the dataset"""

    data_columns: list[DatasetColumnApiInfo]
    """The dataset destination columns."""


class DatasetColumnApiInfo(pydantic.BaseModel):
    """Store column metadata."""

    name: str
    """The column name"""

    data_type: str
    """The type of the data in the column"""

    description: str | None = None
    """The description of the column's content"""


class DatasetAPICredentialError(ValueError):
    """Exception raised when credentials to the API are not found."""


class BearerAuth(requests.auth.AuthBase):
    """Implement Bearer token auth.

    Manages token refresh if token expires.

    If email or passwords are not provided, it looks up their values in the environment
    variable ``ALGOSEEK_DATASET_API_EMAIL`` and ``ALGOSEEK_DATASET_API_PASSWORD`` respectively.

    """

    def __init__(self, config: DatasetAPIConfiguration):
        self.config = config
        self.token: str | None = None
        self._access_token_expiration_date: pendulum.DateTime | None = None
        self._refresh_token_expiration_date: pendulum.DateTime | None = None
        self._authenticate()

    def _authenticate(self) -> None:
        if self.config.email is None or self.config.password is None:
            return

        body = dict()
        if self.config.email is not None:
            body["email"] = self.config.email.get_secret_value()

        if self.config.password is not None:
            body["password"] = self.config.password.get_secret_value()

        headers = {"timeout": "5.0"}
        endpoint = f"{self.config.url}/auth/login"
        response = requests.post(endpoint, json=body, headers=headers)

        if response.status_code != requests.codes.OK:
            msg = f"Authentication failed with code {response.status_code}: {response.json()}"
            raise requests.HTTPError(msg)

        self._update_token_data(response.json())
        logger.info("Authenticated successfully to dataset API.")

    def _refresh(self) -> None:
        body = {"token": self.token}
        headers = {"timeout": "5.0"}
        endpoint = f"{self.config.url}/auth/refresh-token"
        response = requests.post(endpoint, json=body, headers=headers)

        if response.status_code != requests.codes.OK:
            msg = f"access to {endpoint} failed with code {response.status_code}"
            raise requests.HTTPError(msg)

        self._update_token_data(response.json())
        logger.info("Successfully refreshed access token.")

    def _update_token_data(self, response: dict[str, str]) -> None:
        self.token = response["access_token"]
        self._access_token_expiration_date = _api_timestamp_to_datetime(response["access_token_expiry_date"])
        self._refresh_token_expiration_date = _api_timestamp_to_datetime(response["refresh_token_expiry_date"])

    def __call__(self, r):
        """Add auth information to request."""
        now = pendulum.now()

        if self._refresh_token_expiration_date is not None and now > self._refresh_token_expiration_date:
            logger.info("Dataset API token expired and cannot be refreshed. Requesting new access token...")
            self._authenticate()
        elif self._access_token_expiration_date is not None and now > self._access_token_expiration_date:
            logger.info("Dataset API token expired. Refreshing token...")
            self._refresh()

        if self.token is not None:
            r.headers.update({"authorization": f"Bearer {self.token}"})
        return r


def _api_timestamp_to_datetime(s: str) -> pendulum.DateTime:
    dt = pendulum.parse(s)
    assert isinstance(dt, pendulum.DateTime), f"Could not parse {s} as a DateTime object."
    return dt
