"""
Tools to communicate with Algoseek API metadata.

Provides:

BaseAPIConsumer:
  Authenticates to metadata-services.

"""

from dataclasses import dataclass
from os import getenv
from typing import Optional

import requests

ALGOSEEK_API_USERNAME = "ALGOSEEK_API_USERNAME"
ALGOSEEK_API_PASSWORD = "ALGOSEEK_API_PASSWORD"


class BaseAPIConsumer:
    """
    Fetch dataset metadata from metadata services.

    Base URL : https://metadata-services.algoseek.com/api/v1

    Use the `get` method to request data from different endpoints.

    Parameters
    ----------
    user : str or None, default=None
        User name for API login. If ``None``, it tries to get the username
        from the environment variable `ALGOSEEK_API_USERNAME`.
    password : str
        Password for API login. If ``None``, it tries to get the username
        from the environment variable `ALGOSEEK_API_PASSWORD`.

    Raises
    ------
    requests.exceptions.HTTPError
        If authentication fails or the connection times out.

    """

    def __init__(
        self,
        user: Optional[str] = None,
        password: Optional[str] = None,
        base_url: Optional[str] = None,
    ) -> None:
        if base_url is None:
            base_url = "https://metadata-services.algoseek.com/api/v1/"
        self._base_url = base_url
        login_endpoint = self.base_url + "login/access_token/"
        self._token = _get_access_token(login_endpoint, user, password, timeout=5)

    @property
    def base_url(self) -> str:
        """Get the API base URL."""
        return self._base_url

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """
        Request data using the GET method.

        To be used as a base method to request data.

        Manages authentication. Sets a default timeout of 5 s.

        Parameters
        ----------
        endpoint : str
            The endpoint, relative to the base url.
        **kwargs : dict
            Optional parameters to pass to :py:func:`requests.get`

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


@dataclass(frozen=True)
class AuthToken:
    """Container class for auth token data."""

    token: str
    expiry_date: str

    def create_header(self):
        """Create a header to request data."""
        return {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
        }


def _get_access_token(
    login_endpoint: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    **kwargs,
) -> AuthToken:
    """
    Request an access token to the API.

    Parameters
    ----------
    user : str
    password : str
    **kwargs : dict
        Optional parameters passed to :py:func:`requests.post`

    """
    if user is None:
        user = getenv(ALGOSEEK_API_USERNAME)

    if password is None:
        password = getenv(ALGOSEEK_API_PASSWORD)

    if user is None or password is None:
        msg = "User and password must be provided as parameters or as environment variables."
        raise ValueError(msg)
    credentials = {"name": user, "secret": password}
    response = requests.post(login_endpoint, json=credentials, **kwargs)

    if response.status_code == requests.codes.OK:
        json_response = response.json()
        return AuthToken(json_response["token"], json_response["expiry_date"])
    else:
        msg = "Login failed with code {response.status_code}"
        raise requests.HTTPError(msg)
