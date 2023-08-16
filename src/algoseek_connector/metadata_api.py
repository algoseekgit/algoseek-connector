"""
Tools to communicate with Algoseek API metadata.

Provides:

AuthToken
    Login to the metadata API and stores an auth token.

BaseAPIConsumer
    Provides a generic get method to fetch data from the different endpoints.
    Provides functionality to get dataset and datagroup metadata.

"""

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


class AuthToken:
    """
    Create and store an authentication token for API login.

    Parameters
    ----------
    login_endpoint : str
    user : str
    password : str
    **kwargs : dict
        Optional parameters passed to :py:func:`requests.post`

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

        if user is None:
            user = getenv(ALGOSEEK_API_USERNAME)

        if password is None:
            password = getenv(ALGOSEEK_API_PASSWORD)

        if user is None or password is None:
            msg = "User and password must be provided as parameters or as environment variables."
            raise ValueError(msg)
        credentials = {"name": user, "secret": password}
        response = requests.post(login_url, json=credentials, **kwargs)

        if response.status_code == requests.codes.OK:
            json_response = response.json()
            self._token = json_response["token"]
            self._expiry_date = json_response["expiry_date"]
        else:
            msg = "Login failed with code {response.status_code}"
            raise requests.HTTPError(msg)

    @property
    def token(self) -> str:
        """Get the auth token string."""
        return self._token

    def expiry_date(self) -> str:
        """Get the token expiry date."""
        return self._expiry_date

    def create_header(self):
        """Create a header to request data."""
        return {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
        }
