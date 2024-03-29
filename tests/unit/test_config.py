from pathlib import Path

import pytest

from algoseek_connector import config, constants


def test_SettingField_value_no_environment_variable():
    name = "field1"
    value = "value"
    field = config.SettingsField(name, value)
    assert field.get() == value


def test_SettingField_get_value_from_environment_variable(monkeypatch):
    name = "field1"
    value = "value"
    env = "my-test-env-var"
    monkeypatch.setenv(env, value)
    field = config.SettingsField(name, env=env)
    assert field.get() == value


def test_SettingField_with_validator_fails_if_invalid_value_is_passed():
    name = "field1"
    value = ""

    def validator(v):
        is_empty = len(v) == 0
        if is_empty:
            raise ValueError

    with pytest.raises(ValueError):
        config.SettingsField(name, value, validator=validator)


def test_SettingField_set():
    name = "field1"
    old_value = "value1"
    field = config.SettingsField(name, old_value)
    new_value = "value2"
    field.set(new_value)
    assert field.get() == new_value


def test_SettingField_frozen_field_raises_error_when_trying_to_modify():
    name = "field1"
    value = "value1"

    field = config.SettingsField(name, value, frozen=True)

    with pytest.raises(AttributeError):
        field.set("value2")


def test_SettingField__str__():
    name = "field1"
    value = "value1"
    field = config.SettingsField(name, value)
    field_str = str(field)
    assert f"value={value}" in field_str


def test_SettingField__str__secret_value():
    name = "field1"
    value = "value1"
    field = config.SettingsField(name, value, secret=True)
    field_str = str(field)
    assert "value=XXXX" in field_str


def test_SettingGroup_add_no_valid_fields():
    name = "group1"
    fields = list()
    group = config.SettingsGroup(name, fields)

    field_name = "field1"
    field_value = "value1"
    expected = config.SettingsField(field_name, field_value)
    group.add(expected)

    actual = getattr(group, field_name)
    assert actual.name == expected.name
    assert actual.get() == expected.get()


def test_SettingGroup_multiple_add():
    name = "group1"
    group = config.SettingsGroup(name, list())

    for k in range(10):
        field = config.SettingsField(f"field{k}", k)
        group.add(field)

    for k in range(10):
        expected_name = f"field{k}"
        actual = getattr(group, expected_name)
        assert actual.name == expected_name
        assert actual.get() == k


def test_SettingGroup_get_dict_empty_group_returns_empty_dict():
    name = "group1"
    fields = list()
    group = config.SettingsGroup(name, fields)

    expected = dict()
    actual = group.get_dict()
    assert actual == expected


def test_SettingGroup_get_dict():
    name = "group1"
    fields = [config.SettingsField(f"field{k}", k) for k in range(10)]
    group = config.SettingsGroup(name, fields)

    expected = {x.name: x.get() for x in fields}
    actual = group.get_dict()
    assert actual == expected


@pytest.mark.parametrize("v", ["", 1, 10.0, [1, 2, 3]])
def test_validate_non_empty_string_invalid_values_raises_error(v):
    with pytest.raises(ValueError):
        config._validate_non_empty_str(v)


def test_validate_non_empty_string_non_empty_string_does_not_raise_error():
    config._validate_non_empty_str("abcde")


def test_validate_non_empty_string_none_does_not_raise_error():
    config._validate_non_empty_str(None)


def test_validate_ip_address_valid_ip_does_not_raise_error():
    ip = "100.100.1.1"
    config._validate_ip_address(ip)


def test_validate_ip_address_none_does_not_raise_error():
    ip = None
    config._validate_ip_address(ip)


@pytest.mark.parametrize("ip", ["invalid", "100.100"])
def test_validate_ip_address_invalid_ip_raises_value_error(ip):
    with pytest.raises(ValueError):
        config._validate_ip_address(ip)


@pytest.mark.parametrize("x", [1.0, 2, None])
def test_validate_positive_number(x):
    config._validate_positive_number(x)
    assert True


@pytest.mark.parametrize("x", [-1.0, -1, "abc"])
def test_validate_positive_number_invalid(x):
    with pytest.raises(ValueError):
        config._validate_positive_number(x)


def test_create_ardadb_credentials_group_from_empty_dict():
    d = dict()
    group = config._create_ardadb_credentials_group(d)
    assert hasattr(group, constants.ARDADB_HOST_VAR)
    assert hasattr(group, constants.ARDADB_PORT_VAR)
    assert hasattr(group, constants.ARDADB_USERNAME_VAR)
    assert hasattr(group, constants.ARDADB_PASSWORD_VAR)


def test_create_settings_group_from_empty_dict():
    d = dict()
    group = config._create_settings_group(d)
    assert isinstance(group, config.SettingsGroup)


def test_create_ardadb_settings_group_from_empty_dict():
    d = dict()
    group = config._create_ardadb_group(d)
    assert group.name == constants.ARDADB
    assert hasattr(group, constants.SETTINGS_GROUP)
    assert hasattr(group, constants.CREDENTIAL_GROUP)


def test_create_s3_credentials_settings_group_from_empty_dict():
    d = dict()
    group = config._create_s3_credentials_group(d)
    assert group.name == constants.CREDENTIAL_GROUP
    assert hasattr(group, constants.AWS_PROFILE_NAME_VAR)
    assert hasattr(group, constants.AWS_ACCESS_KEY_ID_VAR)
    assert hasattr(group, constants.AWS_SECRET_ACCESS_KEY_VAR)


def test_create_s3_quota_settings_group_from_empty_dict():
    d = dict()
    group = config._create_s3_download_quota_group(d)
    assert group.name == constants.QUOTA_GROUP
    assert hasattr(group, constants.DOWNLOAD_LIMIT_FIELD)
    assert hasattr(group, constants.HARD_DOWNLOAD_FIELD)


def test_create_s3_settings_group_from_empty_dict():
    d = dict()
    group = config._create_s3_settings_group(d)
    assert group.name == constants.S3

    credentials_group = getattr(group, constants.CREDENTIAL_GROUP)
    assert isinstance(credentials_group, config.SettingsGroup)

    settings_group = getattr(group, constants.SETTINGS_GROUP)
    assert isinstance(settings_group, config.SettingsGroup)

    quota_group = getattr(group, constants.QUOTA_GROUP)
    assert isinstance(quota_group, config.SettingsGroup)


def test_create_metadata_services_group_from_empty_dict():
    d = dict()
    group = config._create_metadata_services_settings_group(d)

    assert group.name == constants.METADATA_SERVICE_SETTINGS_GROUP
    assert hasattr(group, constants.METADATA_SERVICES_USER_VAR)
    assert hasattr(group, constants.METADATA_SERVICES_PASSWORD_VAR)


def test_create_default_settings_file(tmp_path: Path):
    destination = tmp_path / "dir1" / "dir2"
    assert not destination.exists()
    config.create_config_file(destination)
    assert destination.exists()


def test_Setting_config_file_settings_files_does_not_exists(tmp_path: Path):
    destination = tmp_path / constants.CONFIG_FILENAME
    conf = config.Settings(destination)
    assert not destination.exists()
    assert isinstance(conf, config.Settings)
    assert hasattr(conf, constants.S3)
    assert hasattr(conf, constants.ARDADB)


def test_Setting_is_singleton(tmp_path: Path):
    destination = tmp_path / constants.CONFIG_FILENAME
    conf1 = config.Settings(destination)
    conf2 = config.Settings(destination)
    assert conf1 is conf2


def test_read_config_file_test_setting_groups(tmp_path: Path):
    destination = tmp_path / constants.CONFIG_FILENAME
    config.create_config_file(destination)
    conf = config.Settings(destination)
    assert isinstance(conf, config.Settings)
    assert hasattr(conf, constants.S3)
    assert hasattr(conf, constants.ARDADB)


def test_read_config_file_test_ardadb_group(tmp_path: Path):
    destination = tmp_path / constants.CONFIG_FILENAME
    config.create_config_file(destination)
    conf = config.Settings(destination)
    ardadb_group = getattr(conf, constants.ARDADB)
    assert isinstance(ardadb_group, config.SettingsGroup)
    assert hasattr(ardadb_group, constants.CREDENTIAL_GROUP)
    assert hasattr(ardadb_group, constants.SETTINGS_GROUP)


def test_read_config_file_test_s3_group(tmp_path: Path):
    destination = tmp_path / constants.CONFIG_FILENAME
    config.create_config_file(destination)
    conf = config.Settings(destination)
    ardadb_group = getattr(conf, constants.S3)
    assert isinstance(ardadb_group, config.SettingsGroup)
    assert hasattr(ardadb_group, constants.CREDENTIAL_GROUP)
    assert hasattr(ardadb_group, constants.SETTINGS_GROUP)
    assert hasattr(ardadb_group, constants.QUOTA_GROUP)
