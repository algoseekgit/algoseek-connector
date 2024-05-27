import os
from unittest import mock

import pytest
import requests
from algoseek_connector.base import InvalidDataGroupName
from algoseek_connector.dataset_api import BearerAuth, DatasetAPIProvider
from algoseek_connector.settings import AlgoseekConnectorSettings


@pytest.fixture
def mock_env():
    with mock.patch.dict(os.environ, clear=True):
        yield


class TestBearerAuth:
    def test_token_is_none_if_no_email(self):
        config = AlgoseekConnectorSettings().dataset_api
        config.email = None
        auth = BearerAuth(config)
        assert auth.token is None

    def test_token_is_none_if_no_password(self):
        config = AlgoseekConnectorSettings().dataset_api
        config.password = None
        auth = BearerAuth(config)
        assert auth.token is None

    def test_invalid_credentials_raise_error(self, mock_env):
        os.environ["ALGOSEEK__DATASET_API__EMAIL"] = "mock@email.com"
        os.environ["ALGOSEEK__DATASET_API__PASSWORD"] = "mock_pass"
        config = AlgoseekConnectorSettings().dataset_api
        with pytest.raises(requests.HTTPError):
            BearerAuth(config)

    def test_login_ok(self):
        # uses credentials defined in the environment
        config = AlgoseekConnectorSettings().dataset_api
        BearerAuth(config)


class TestDatasetAPI:
    @pytest.fixture(scope="class")
    def api(self):
        return DatasetAPIProvider()

    def test_list_data_groups(self, api: DatasetAPIProvider):
        groups = api.list_data_groups()
        assert len(groups)
        assert all(isinstance(x, str) for x in groups)

    def test_get_data_group_ok(self, api: DatasetAPIProvider):
        for group in api.list_data_groups():
            api.get_data_group(group)

    def test_get_data_group_invalid_group_name_raises_error(self, api: DatasetAPIProvider):
        with pytest.raises(InvalidDataGroupName):
            api.get_data_group("invalid-group-name")

    def test_list_dataset_destinations_ok(self, api: DatasetAPIProvider):
        datasets = api.list_dataset_destinations()
        assert len(datasets)
        assert all(isinstance(x, int) for x in datasets)

    def test_get_dataset_destinations_ok(self, api: DatasetAPIProvider):
        destinations = api.list_dataset_destinations()
        for destination_id in destinations:
            api.get_dataset(destination_id)
