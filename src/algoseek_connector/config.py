"""
Settings utilities.

Reads settings from user settings files.

"""

from __future__ import annotations

import os
import shutil
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Union

import tomli

from . import utils

CONFIG_FILENAME = "config.toml"
DEFAULT_CONFIG_FILENAME = "default-config.toml"
SECRETS_FILENAME = "secrets.toml"
ALGOSEEK_ARDADB_HOST_ENV = "ALGOSEEK_ARDADB_HOST"
ALGOSEEK_ARDADB_PORT_ENV = "ALGOSEEK_ARDADB_PORT"
ALGOSEEK_ARDADB_USERNAME_ENV = "ALGOSEEK_ARDADB_USERNAME"
ALGOSEEK_ARDADB_PASSWORD_ENV = "ALGOSEEK_ARDADB_PASSWORD"
ALGOSEEK_AWS_PROFILE = "ALGOSEEK_AWS_PROFILE"
ALGOSEEK_AWS_ACCESS_KEY_ID = "ALGOSEEK_AWS_ACCESS_KEY_ID"
ALGOSEEK_AWS_SECRET_ACCESS_KEY = "ALGOSEEK_AWS_SECRET_ACCESS_KEY"

# TODO: extact strings as variables
# TODO: test singleton and reload


TIB = 1024**4  # 1 tebibyte


class _SettingsMeta(type):
    """Settings meta class. Implements constructor as a singleton."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Settings(metaclass=_SettingsMeta):
    """
    Container class for user settings. Implemented as a singleton.

    Parameters
    ----------
    path : Path or None, default=None
        Path to settings file. If no path is provided, uses
        `~/.algoseek/config.toml`.

    """

    def __init__(self, path: Optional[Path] = None):
        if path is None:
            path = get_config_file_path()

        if path.is_file():
            self._path = path
            with open(path, "rb") as f:
                config_dict = tomli.load(f)
        else:
            self._path = None
            config_dict = dict()

        for g in create_settings_group_from_dictionary(config_dict):
            setattr(self, g.name, g)


class SettingsGroup:
    """Container class for groups of setting options."""

    def __init__(
        self, name: str, fields: Sequence[Union[SettingsGroup, SettingsField]]
    ):
        self._name = name
        self._dict = dict()
        for f in fields:
            self.add(f)

    @property
    def name(self):
        """Get the settings group name."""
        return self._name

    def add(self, field: Union[SettingsGroup, SettingsField]):
        """Add field to the group."""
        setattr(self, field.name, field)
        self._dict[field.name] = field

    def get_dict(self) -> dict[str, Any]:
        """Get the settings group as a dictionary."""
        d = dict()
        for k, v in self._dict.items():
            if isinstance(v, SettingsGroup):
                d[k] = v.get_dict()
            else:
                d[k] = v.get()
        return d


class SettingsField:
    """
    Base class for setting fields.

    Parameters
    ----------
    name : str
        The setting field name.
    value : any
        The setting field value.
    description : str or None, default=None
        The setting field description. If ``None`` sets an empty string.
    secret : bool, default=False
        Wether the setting field contains sensitive information.
    frozen : bool, default=False
        If ``False``, the value can be modified after construction. Otherwise,
        the value is frozen.
    env : str or None, default=None
        Environment variable name to retrieve the field value if the value
        provided is ``None``.
    validator : callable or None, default=None
        A function to validate the value provided that raises a ValueError if
        an invalid value is passed. If ``None``, no validation is applied. The
        function must have the following shape:

        .. code-block:: python

            def validator(v):
                is_valid = True  # do some value tests
                if not is_valid:
                    raise ValueError

    """

    def __init__(
        self,
        name: str,
        value: Any = None,
        description: Optional[str] = None,
        secret: bool = False,
        frozen: bool = False,
        env: Optional[str] = None,
        validator: Optional[Callable] = None,
    ):
        self._name = name
        self._env = env
        self._secret = secret
        self._frozen = frozen
        self.description = description

        if validator is None:

            def validate(v):
                pass

            validator = validate

        self._validator = validator
        validator(value)
        self._value = value

    def __str__(self):
        v = "XXXX" if self._secret else self.get()
        return f"SettingField(name={self.name}, value={v})"

    def is_secret(self) -> bool:
        """Check if the current field is secret."""
        return self._secret

    @property
    def name(self) -> str:
        """Get the settings field name."""
        return self._name

    @property
    def description(self) -> str:
        """Get the settings field description."""
        return self._description

    @description.setter
    def description(self, value: Optional[str]):
        if value is None:
            value = ""
        self._description = value

    @property
    def frozen(self) -> bool:
        """Check if the field is frozen."""
        return self._frozen

    def get(self) -> Any:
        """Get the field value."""
        v = self._value
        if v is None and self._env is not None:
            v = os.getenv(self._env)
        return v

    def set(self, v):
        """
        Set the field value.

        If an environment variable is defined, retrieves the value from it.
        """
        if self.frozen:
            msg = "This attribute is frozen and can only be modified from "
            raise AttributeError(msg)
        self._validator(v)
        self._value = v


def get_config_file_path() -> Path:
    """Get the path to the algoseek config file."""
    algoseek_path = utils.get_algoseek_path()
    return algoseek_path / CONFIG_FILENAME


def create_default_config_file(destination: Optional[Path] = None):
    """
    Create a default configuration file.

    Parameters
    ----------
    destination : Path or None, default=None
        The Path to create the configuration file. If no path is provided,
        the file is created at `~/.algoseek/config.yaml`.

    """
    if destination is None:
        destination = get_config_file_path()
    parent_dir = destination.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    default_file_path = Path(__file__).parent / DEFAULT_CONFIG_FILENAME
    shutil.copy(default_file_path, destination)


def create_settings_group_from_dictionary(config_dict: dict) -> Sequence[SettingsGroup]:
    """Create a setting instance."""
    settings_groups = list()

    ardadb_dict = config_dict.get("ardadb", dict())
    ardadb_group = _create_ardadb_group(ardadb_dict)
    settings_groups.append(ardadb_group)

    s3_dict = config_dict.get("s3", dict())
    s3_group = _create_s3_settings_group(s3_dict)
    settings_groups.append(s3_group)

    return settings_groups


def _create_s3_settings_group(s3_dict: dict) -> SettingsGroup:
    credentials_dict = s3_dict.get("credentials", dict())
    settings_dict = s3_dict.get("settings", dict())
    quota_dict = s3_dict.get("quota", dict())
    fields = [
        _create_s3_credentials_group(credentials_dict),
        _create_settings_group(settings_dict),
        _create_s3_download_quota_group(quota_dict),
    ]
    return SettingsGroup("s3", fields)


def _create_s3_download_quota_group(quota_dict: dict) -> SettingsGroup:
    field_parameters = [
        {
            "name": "download_limit",
            "description": "Set the maximum download quota for S3 datasets.",
            "validator": _validate_positive_number,
            "value": TIB,
        },
        {
            "name": "download_limit_do_not_change",
            "description": "A second download limit fo S3 datasets.",
            "validator": _validate_positive_number,
            "frozen": True,
            "value": 20 * TIB,
        },
    ]

    for p in field_parameters:
        p["value"] = quota_dict.get(p["name"], p["value"])
    fields = [SettingsField(**p) for p in field_parameters]
    return SettingsGroup("quota", fields)


def _create_s3_credentials_group(credentials_dict: dict) -> SettingsGroup:
    field_parameters = [
        {
            "name": "profile_name",
            "description": "A profile stored in `~/.aws/credentials` with access to Algoseek datasets.",
            "env": ALGOSEEK_AWS_PROFILE,
            "validator": _validate_non_empty_str,
            "secret": False,
        },
        {
            "name": "aws_access_key_id",
            "description": "The AWS access key associated with an IAM user or role.",
            "env": ALGOSEEK_AWS_ACCESS_KEY_ID,
            "validator": _validate_non_empty_str,
            "secret": False,
        },
        {
            "name": "aws_secret_access_key",
            "description": "Thee secret key associated with the access key.",
            "env": ALGOSEEK_AWS_SECRET_ACCESS_KEY,
            "validator": _validate_non_empty_str,
            "secret": True,
        },
    ]

    # set values from TOML file dict
    if credentials_dict is not None:
        for p in field_parameters:
            p["value"] = credentials_dict.get(p["name"])
    fields = [SettingsField(**p) for p in field_parameters]
    group = SettingsGroup("credentials", fields)
    return group


def _create_ardadb_group(d: dict) -> SettingsGroup:
    credentials_dict = d.get("credentials", dict())
    settings_dict = d.get("settings", dict())
    fields = [
        _create_ardadb_credentials_group(credentials_dict),
        _create_settings_group(settings_dict),
    ]
    return SettingsGroup("ardadb", fields)


def _create_ardadb_credentials_group(credentials_dict: dict) -> SettingsGroup:
    field_parameters = [
        {
            "name": "host",
            "description": "The ArdaDB host Address.",
            "env": ALGOSEEK_ARDADB_HOST_ENV,
            "validator": _validate_ip_address,
            "secret": True,
        },
        {
            "name": "port",
            "description": "The ArdaDB connection port.",
            "env": ALGOSEEK_ARDADB_PORT_ENV,
            "validator": _validate_port,
            "secret": True,
        },
        {
            "name": "user",
            "description": "The ArdaDB user name.",
            "env": ALGOSEEK_ARDADB_USERNAME_ENV,
            "validator": _validate_non_empty_str,
            "secret": True,
        },
        {
            "name": "password",
            "description": "The ArdaDB user' password.",
            "env": ALGOSEEK_ARDADB_PASSWORD_ENV,
            "validator": _validate_non_empty_str,
            "secret": True,
        },
    ]

    # set values from TOML file dict
    if credentials_dict is not None:
        for p in field_parameters:
            p["value"] = credentials_dict.get(p["name"])
    fields = [SettingsField(**p) for p in field_parameters]
    group = SettingsGroup("credentials", fields)
    return group


def _create_settings_group(settings_dict: dict) -> SettingsGroup:
    """Create a settings group both for ArdaDB and S3."""
    fields = [SettingsField(k, value=v) for k, v in settings_dict.items()]
    group = SettingsGroup("settings", fields)
    return group


def _validate_non_empty_str(s: Optional[str]):
    """Check if the value passed is a non-empty string. Raise ValueError otherwise."""
    is_non_empty_string = isinstance(s, str) and len(s)
    if not (s is None or is_non_empty_string):
        msg = f"Expected ``None`` or a non-empty string. Gor {s}."
        raise ValueError(msg)


def _validate_ip_address(ip: Optional[str]):
    """Check if the value passed is a valid IP address. Raise ValueError otherwise."""
    if ip is not None:
        ip_address(ip)  # raises a value error if the ip is not valid


def _validate_port(p: int):
    """Check if the value passed is a valid port. Raise ValueError otherwise."""
    is_positive_int = isinstance(p, int) and p > 0
    if not (p is None or is_positive_int):
        msg = f"port must be a positive integer. Got {p}."
        raise ValueError(msg)


def _validate_positive_number(x: Union[float, int]):
    is_positive_number = isinstance(x, (int, float)) and x > 0
    if not (x is None or is_positive_number):
        msg = f"Expected a positive number. Got {x}."
        raise ValueError(msg)
