import pytest
import requests

from algoseek_connector.clickhouse import metadata_api
from algoseek_connector.metadata_api import ALGOSEEK_API_PASSWORD, ALGOSEEK_API_USERNAME


@pytest.fixture(scope="module")
def api_consumer():
    return metadata_api.APIConsumer()


def test_APIConsumer_no_user_provided(monkeypatch):
    password = "InvalidPassword"

    monkeypatch.delenv(ALGOSEEK_API_USERNAME)

    with pytest.raises(ValueError):
        metadata_api.APIConsumer(password=password)


def test_APIConsumer_no_password_provided(monkeypatch):
    user = "mock-user"

    monkeypatch.delenv(ALGOSEEK_API_PASSWORD)

    with pytest.raises(ValueError):
        metadata_api.APIConsumer(user=user)


def test_APIConsumer_auth_error():
    user = "InvalidUsername"
    password = "InvalidPassword"
    with pytest.raises(requests.HTTPError):
        metadata_api.APIConsumer(user, password)
