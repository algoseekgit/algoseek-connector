from datetime import datetime

import pytest
from requests.exceptions import HTTPError

from algoseek_connector import metadata_api
from algoseek_connector.base import InvalidDataGroupName, InvalidDataSetName
from algoseek_connector.metadata_api import AuthToken, BaseAPIConsumer


def test_AuthToken_no_user_provided(monkeypatch):
    password = "InvalidPassword"

    monkeypatch.delenv(metadata_api.ALGOSEEK_API_USERNAME)

    with pytest.raises(ValueError):
        AuthToken(password=password)


def test_AuthToken_no_password_provided(monkeypatch):
    user = "mock-user"

    monkeypatch.delenv(metadata_api.ALGOSEEK_API_PASSWORD)

    with pytest.raises(ValueError):
        AuthToken(user=user)


def test_AuthToken_auth_error():
    user = "InvalidUsername"
    password = "InvalidPassword"
    with pytest.raises(HTTPError):
        AuthToken(user=user, password=password)


@pytest.fixture(scope="module")
def auth_token():
    return AuthToken()


def test_AuthToken_expiration_date():
    now = datetime.utcnow()
    token = AuthToken()
    assert token.expiry_date > now


def test_AuthToken_refresh():
    token = AuthToken()
    # set a dummy expiration date
    token._expiry_date = datetime.utcnow()
    now = datetime.utcnow()
    token.refresh()
    assert now < token.expiry_date


def test_AuthToken_refresh_raises_value_error_if_user_env_variable_is_not_set(
    monkeypatch,
):
    token = AuthToken()
    # set a dummy expiration date
    token._expiry_date = datetime.utcnow()
    monkeypatch.delenv(metadata_api.ALGOSEEK_API_USERNAME)
    with pytest.raises(ValueError):
        token.refresh()


def test_AuthToken_refresh_raises_value_error_if_password_env_variable_is_not_set(
    monkeypatch,
):
    token = AuthToken()
    # set a dummy expiration date
    token._expiry_date = datetime.utcnow()
    monkeypatch.delenv(metadata_api.ALGOSEEK_API_PASSWORD)
    with pytest.raises(ValueError):
        token.refresh()


@pytest.fixture(scope="module")
def api_consumer(auth_token):
    return BaseAPIConsumer(auth_token)


def test_invalid_endpoint_raises_http_error(api_consumer: BaseAPIConsumer):
    endpoint = "InvalidEndpoint"
    with pytest.raises(HTTPError):
        api_consumer.get(endpoint)


def test_list_datasets(api_consumer: BaseAPIConsumer):
    datasets = api_consumer.list_datasets()
    assert all(isinstance(x, str) for x in datasets)


def test_get_dataset_metadata_using_text_id(api_consumer: BaseAPIConsumer):
    expected_keys = [
        "text_id",
        "full_name",
        "display_name",
        "description",
        "long_description",
        "creation_date",
        "start_date",
        "end_date",
        "price_per_year",
        "price_per_symbol",
        "listing_priority",
        "tags",
        "datagroup_id",
        "data_format_id",
        "data_class_id",
        "data_type_id",
        "time_granularity_id",
        "region_id",
        "status_id",
        "publisher_id",
        "documentation_id",
        "delivery_methods",
        "id",
        "cloud_storage",
        "database_table",
    ]
    for dataset_name in api_consumer.list_datasets():
        dataset_metadata = api_consumer.get_dataset_metadata(dataset_name)
        assert all(x in dataset_metadata for x in expected_keys)


def test_get_dataset_metadata_using_id(api_consumer: BaseAPIConsumer):
    for dataset_name in api_consumer.list_datasets():
        dataset_metadata = api_consumer.get_dataset_metadata(dataset_name)
        dataset_id = dataset_metadata["id"]
        dataset_metadata_by_id = api_consumer.get_dataset_metadata(dataset_id)
        assert dataset_metadata == dataset_metadata_by_id


def test_get_api_metadata_invalid_text_id(api_consumer: BaseAPIConsumer):
    text_id = "InvalidTextId"
    with pytest.raises(InvalidDataSetName):
        api_consumer.get_dataset_metadata(text_id)


def test_get_api_metadata_invalid_id(api_consumer: BaseAPIConsumer):
    id_ = -10
    with pytest.raises(ValueError):
        api_consumer.get_dataset_metadata(id_)


def test_list_datagroups(api_consumer: BaseAPIConsumer):
    datasets = api_consumer.list_datagroups()
    assert all(isinstance(x, str) for x in datasets)


def test_get_datagroup_metadata_by_text_id(api_consumer: BaseAPIConsumer):
    expected_keys = [
        "description",
        "display_name",
        "full_name",
        "id",
        "pricing_id",
        "region_id",
        "status_id",
        "symbols_per_component",
        "tags",
        "text_id",
        "universe_components",
        "universe_description",
        "universe_size",
    ]
    for text_id in api_consumer.list_datagroups():
        data_group_metadata = api_consumer.get_datagroup_metadata(text_id)
        assert all(x in data_group_metadata for x in expected_keys)


def test_get_datagroup_metadata_by_id(api_consumer: BaseAPIConsumer):
    for text_id in api_consumer.list_datagroups():
        data_group_metadata = api_consumer.get_datagroup_metadata(text_id)
        id_ = data_group_metadata["id"]
        data_group_metadata_by_id = api_consumer.get_datagroup_metadata(id_)
        assert data_group_metadata == data_group_metadata_by_id


def test_get_datagroup_metadata_invalid_text_id(api_consumer: BaseAPIConsumer):
    text_id = "InvalidDataGroupId"
    with pytest.raises(InvalidDataGroupName):
        api_consumer.get_datagroup_metadata(text_id)


def test_get_datagroup_metadata_invalid_id(api_consumer: BaseAPIConsumer):
    id_ = -10
    with pytest.raises(ValueError):
        api_consumer.get_datagroup_metadata(id_)


def test_get_dataset_documentation(api_consumer: BaseAPIConsumer):
    expected_keys = [
        "description",
        "file_name",
        "html",
        "id",
        "s3_location_id",
        "text_id",
        "title",
    ]
    text_id = "fut_opt_trades"
    docs = api_consumer.get_documentation(text_id)
    assert all(x in docs for x in expected_keys)


def test_get_dataset_documentation_docs_not_available(
    api_consumer: BaseAPIConsumer,
):
    text_id = "eq_daily_oc_price"
    with pytest.raises(ValueError):
        api_consumer.get_documentation(text_id)
