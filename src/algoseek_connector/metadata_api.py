"""
Tools to communicate with Algoseek API metadata.

Provides:

AuthToken
    Login to the metadata API and stores an auth token.

BaseAPIConsumer
    Provides a generic get method to fetch data from the different endpoints.
    Provides functionality to get dataset and datagroup metadata.

"""
from datetime import datetime
from functools import lru_cache
from os import getenv
from typing import Any, Optional, Union

import requests

from .base import InvalidDataGroupName, InvalidDataSetName

ALGOSEEK_API_USERNAME = "ALGOSEEK_API_USERNAME"
ALGOSEEK_API_PASSWORD = "ALGOSEEK_API_PASSWORD"
BASE_URL = "https://metadata-services.algoseek.com/api/v1/"


class BaseAPIConsumer:
    """
    Fetch dataset metadata from metadata services.

    Base URL : https://metadata-services.algoseek.com/api/v1

    Use the `get` method to request data from different endpoints.

    Parameters
    ----------
    token : AuthToken
        The API authentication token.

    Raises
    ------
    requests.exceptions.HTTPError
        If authentication fails or the connection times out.

    Methods
    -------
    get:
        Request data from an endpoint using the GET method.
    list_datagroups:
        List available data groups.
    list_datasets:
        List available datasets.
    get_dataset_metadata:
        Get the metadata of a dataset.
    get_datagroup_metadata:
        Get the metadata of a data group.
    get_documentation:
        Get the documentation metadata of a dataset.
    get_time_granularity_metadata:
        Get the time granularity metadata.
    get_platform_dataset_metadata:
        Get the dataset metadata used in the platform frontend.

    """

    def __init__(self, token: "AuthToken", base_url: Optional[str] = None) -> None:
        if base_url is None:
            base_url = BASE_URL
        self._base_url = base_url
        self._token = token

    @property
    def base_url(self) -> str:
        """Get the API base URL."""
        return self._base_url

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Request data using the GET method.

        To be used as a base method to request data.

        Sets a default timeout of 5 s.

        Parameters
        ----------
        endpoint : str
            The endpoint, relative to the base url.
        **kwargs : dict
            Optional parameters to pass to :py:func:`requests.get`

        Raises
        ------
        requests.exceptions.HTTPError
            If a non-existent dataset name is passed.

        """
        self._token.refresh()
        url = self.base_url + endpoint
        kwargs["headers"] = self._token.create_header()
        kwargs.setdefault("timeout", 5)
        response = requests.get(url, **kwargs)

        if response.status_code == requests.codes.OK:
            return response
        else:
            msg = f"Connection to API failed with code {response.status_code}"
            raise requests.exceptions.HTTPError(msg)

    @lru_cache
    def _fetch_dataset_metadata(self) -> dict[str, dict]:
        """
        Fetch metadata from all datasets.

        Returns a dictionary that maps datasets text_id to dataset metadata.
        """
        # _fetch methods retrieves all metadata from the API so future calls
        # are simply a dictionary lookup.
        endpoint = "public/dataset/"
        response = self.get(endpoint)
        return {x["text_id"]: x for x in response.json()}

    @lru_cache
    def _fetch_datagroup_metadata(self) -> dict[str, dict]:
        """
        Fetch metadata from all datagroups.

        Returns a dictionary that maps datagroups text_id to datagroup metadata.
        """
        endpoint = "public/data_group/"
        response = self.get(endpoint)
        return {x["text_id"]: x for x in response.json()}

    @lru_cache
    def _fetch_dataset_platform_metadata(self) -> dict[str, dict]:
        """
        Fetch platform metadata for all datasets.

        Returns a dictionary that maps datasets text_id to dataset metadata.
        """
        endpoint = "public/dataset/platform/frontend/"
        return {x["text_id"]: x for x in self.get(endpoint).json()}

    @lru_cache
    def _data_group_id_to_text_id(self) -> dict[int, str]:
        """Create a dict that maps datagroups id to text ids."""
        metadata = self._fetch_datagroup_metadata()
        return {x["id"]: x["text_id"] for x in metadata.values()}

    @lru_cache
    def _dataset_id_to_text_id(self) -> dict[int, str]:
        """Create a dict that maps datasets id to text ids."""
        metadata = self._fetch_dataset_metadata()
        return {x["id"]: x["text_id"] for x in metadata.values()}

    def list_datagroups(self) -> list[str]:
        """Retrieve the text id of all datagroups."""
        return list(self._fetch_datagroup_metadata())

    def list_datasets(self) -> list[str]:
        """Retrieve the text id of all datasets."""
        return list(self._fetch_dataset_metadata())

    def get_datagroup_metadata(self, id_: Union[int, str]) -> dict[str, Any]:
        """
        Retrieve metadata from a datagroup.

        Parameters
        ----------
        id_ : int or str
            The data group id. If an int is passed, it is interpreted as the
            numerical id of the data group. If an str is passed, it is
            interpreted as the text id of the data group.

        Returns
        -------
        dict
            See the documentation for the `api/v1/public/data_group/{text_id}`
            endpoint at https://metadata-services.algoseek.com/docs for a
            description of the expected format.

        Raises
        ------
        InvalidDataGroupName
            If an invalid text id is passed.
        ValueError
            If an invalid id is passed.

        See Also
        --------
        :py:func:`~algoseek_connector.BaseAPIConsumer.list_datagroups`
            Provides a list text ids from available datagroups.

        """
        if isinstance(id_, int):
            try:
                text_id = self._data_group_id_to_text_id()[id_]
            except KeyError:
                msg = f"{id_} is not a valid datagroup id."
                raise ValueError(msg) from None
        else:
            text_id = id_

        try:
            return self._fetch_datagroup_metadata()[text_id]
        except KeyError:
            raise InvalidDataGroupName(text_id) from None

    def get_dataset_metadata(self, id_: Union[int, str]) -> dict[str, Any]:
        """
        Retrieve metadata from a dataset.

        Parameters
        ----------
        id_ : int or str
            The dataset id. If an int is passed, it is interpreted as the
            numerical id of the dataset. If a str is passed, it is
            interpreted as the text id of the dataset.

        Returns
        -------
        dict
            See the documentation for the `api/v1/public/dataset/{text_id}`
            endpoint at https://metadata-services.algoseek.com/docs for a
            description of the expected format.

        Raises
        ------
        InvalidDatasetName
            If an invalid text id is passed.
        ValueError
            If an invalid id is passed.

        See Also
        --------
        :py:func:`~algoseek_connector.BaseAPIConsumer.list_datasets`
            Provides a list text ids from available datasets.

        """
        if isinstance(id_, int):
            try:
                text_id = self._dataset_id_to_text_id()[id_]
            except KeyError:
                msg = f"{id_} is not a valid dataset id."
                raise ValueError(msg) from None
        else:
            text_id = id_

        try:
            return self._fetch_dataset_metadata()[text_id]
        except KeyError:
            raise InvalidDataSetName(text_id) from None

    def get_documentation(self, text_id: str) -> dict[str, Any]:
        """
        Retrieve the documentation metadata from a dataset.

        Parameters
        ----------
        text_id : str
            The text id of a dataset.

        Raises
        ------
        InvalidDatasetName
            If an invalid text id is passed.
        ValueError
            If no documentation is available for the dataset

        """
        dataset_metadata = self.get_dataset_metadata(text_id)
        documentation_id = dataset_metadata["documentation_id"]
        if documentation_id is None:
            msg = f"Documentation not available for dataset with text_id={text_id}."
            raise ValueError(msg)
        endpoint = f"public/documentation/{documentation_id}/"
        return self.get(endpoint).json()

    @lru_cache
    def get_time_granularity_metadata(self, id_: int) -> dict[str, Any]:
        """
        Retrieve time granularity metadata.

        Parameters
        ----------
        id_ : int
            The granularity id.

        Raises
        ------
        HTTPError
            If a non-existent id is passed.

        """
        endpoint = f"public/time_granularity/{id_}/"
        return self.get(endpoint).json()

    def get_platform_dataset_metadata(self, text_id: str) -> dict[str, Any]:
        """
        Retrieve dataset metadata used in the platform.

        Parameters
        ----------
        text_id : str
            The text id of a dataset.

        Raises
        ------
        ValueError
            If metadata is not available for the specified text id.

        """
        platform_metadata = self._fetch_dataset_platform_metadata()
        try:
            return platform_metadata[text_id]
        except KeyError:
            msg = f"Platform metadata not available for dataset {text_id}."
            raise ValueError(msg)


class AuthToken:
    """
    Create and store an authentication token for API login.

    Parameters
    ----------
    login_url : str or None
        The login endpoint. If ``None``, the default login URL is used.
    user : str
        The user name to login into the API. If ``None``, the user name is
        retrieved from the environment variable `ALGOSEEK_API_USERNAME`.
    password : str
        The User password. If ``None``, the password is retrieved from the
        environment variable `ALGOSEEK_API_PASSWORD`.
    **kwargs : dict
        Optional parameters passed to :py:func:`requests.post`

    Methods
    -------
    create_header:
        Create a header with credentials to make queries.
    refresh:
        Update the token using credentials stored on environment variables.

    """

    def __init__(
        self,
        login_url: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs,
    ) -> None:
        if login_url is None:
            login_url = BASE_URL + "login/access_token/"

        self._login_url = login_url

        if user is None:
            user = getenv(ALGOSEEK_API_USERNAME)

        if password is None:
            password = getenv(ALGOSEEK_API_PASSWORD)

        if user is None or password is None:
            msg = "User and password must be provided as parameters or as environment variables."
            raise ValueError(msg)

        login_metadata = _get_login_metadata(user, password, login_url)
        self._token = login_metadata["token"]
        exp_date_str = login_metadata["expiry_date"]
        self._expiry_date = datetime.fromisoformat(exp_date_str)

    @property
    def token(self) -> str:
        """Get the auth token string."""
        return self._token

    @property
    def expiry_date(self) -> datetime:
        """Get the token expiry date."""
        return self._expiry_date

    def create_header(self):
        """Create a header to request data."""
        return {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
        }

    def refresh(self):
        """Try to obtain a new token using credentials stored on env variables."""
        if self.expiry_date < datetime.utcnow():
            user = getenv(ALGOSEEK_API_USERNAME)
            if user is None:
                msg = (
                    "Automatic metadata API token refresh failed. User name must "
                    "be set in the environment variable ALGOSEEK_API_USER."
                )
                raise ValueError(msg)
            password = getenv(ALGOSEEK_API_PASSWORD)

            if password is None:
                msg = (
                    "Automatic metadata API token refresh failed. Password must "
                    "be set in the environment variable ALGOSEEK_API_PASSWORD."
                )
                raise ValueError(msg)
            login_metadata = _get_login_metadata(user, password, self._login_url)
            self._token = login_metadata["token"]
            exp_date_str = login_metadata["expiry_date"]
            self._expiry_date = datetime.fromisoformat(exp_date_str)


def _get_login_metadata(user: str, password: str, url: str, **kwargs) -> dict[str, str]:
    credentials = {"name": user, "secret": password}
    response = requests.post(url, json=credentials, **kwargs)

    if response.status_code == requests.codes.OK:
        return response.json()
    else:
        msg = "Login failed with code {response.status_code}"
        raise requests.HTTPError(msg)
