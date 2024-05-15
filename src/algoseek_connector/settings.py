"""Algoseek connector settings."""

import pathlib

import tomli
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import models
from .utils import get_algoseek_path


class AlgoseekConnectorSettings(BaseSettings):
    """Store library configuration."""

    model_config = SettingsConfigDict(env_prefix="ALGOSEEK__", env_nested_delimiter="__", validate_assignment=True)

    dataset_api: models.DatasetAPIConfiguration = models.DatasetAPIConfiguration()
    """Dataset API settings"""

    ardadb: models.ArdaDBConfiguration = models.ArdaDBConfiguration()
    """ArdaDB settings"""

    s3: models.S3Configuration = models.S3Configuration()
    """S3 data source settings."""


def get_settings_file_path() -> pathlib.Path:
    """Get the path to the tomli config file."""
    return get_algoseek_path() / "config.toml"


def load_settings() -> AlgoseekConnectorSettings:
    """Load the Algoseek connector settings."""
    path = get_settings_file_path()
    if path.is_file():
        with path.open("rb") as f:
            # TODO: test invalid TOML file
            config_dict = tomli.load(f)
    else:
        config_dict = dict()

    return AlgoseekConnectorSettings(**config_dict)
