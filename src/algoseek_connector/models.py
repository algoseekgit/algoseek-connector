"""Algoseek connector data models."""

from __future__ import annotations

import enum
from typing import Any

import pydantic

from .utils import b64_decode

TIB = 1024**4  # 1 tebibyte


class DataSourceType(str, enum.Enum):
    """Available data source types."""

    ARDADB = "ArdaDB"
    """The ArdaDB data source."""

    S3 = "S3"
    """The S3 data source."""


class BaseConfigModel(pydantic.BaseModel):
    """Base config model."""

    model_config = pydantic.ConfigDict(validate_assignment=True)


class DatasetAPIConfiguration(BaseConfigModel):
    """Store dataset API configuration."""

    url: str = "https://datasets-metadata.algoseek.com/api/v2"
    """The API base URL"""

    headers: dict[str, str] | None = None
    """Headers to include in all requests."""

    email: pydantic.SecretStr | None = None
    """the email to request an access token."""

    password: pydantic.SecretStr | None = None
    """the password to request an access token."""

    @pydantic.field_validator("email", "password")
    @classmethod
    def _cast_to_secret_str(cls, value) -> pydantic.SecretStr | None:
        if isinstance(value, str):
            value = pydantic.SecretStr(value)
        return value

    @pydantic.model_validator(mode="before")
    def _set_credentials(cls, data: Any) -> Any:
        """Set API credentials. Defaults are set here to avoid showing default credentials in API docs."""
        # obfuscated email and password
        data.setdefault("email", b64_decode("Y29ubmVjdG9yLWxpYkBhbGdvc2Vlay5jb20="))
        data.setdefault("password", b64_decode("NTd4Ql9kNjlVX01xZ3FfdXpyUA=="))
        return data


class ArdaDBConfiguration(BaseConfigModel):
    """Store ArdaDB data source configuration."""

    host: str = "0.0.0.0"
    """The ArdaDB host Address"""

    port: pydantic.PositiveInt = 8123
    """The ArdaDB connection port"""

    user: pydantic.SecretStr = pydantic.SecretStr("")
    """The ArdaDB user name"""

    password: pydantic.SecretStr = pydantic.SecretStr("")
    """The ArdaDB password"""

    extra: dict = dict()
    """Optional arguments passed to clickhouse_connect.get_client. See
    `here <https://clickhouse.com/docs/en/integrations/python#clickhouse-connect-driver-api>`_
    for a description of the parameters that are accepted.
    """

    @pydantic.field_validator("user", "password")
    @classmethod
    def _cast_to_secret_str(cls, value) -> pydantic.SecretStr | None:
        if isinstance(value, str):
            value = pydantic.SecretStr(value)
        return value


class S3Configuration(BaseConfigModel):
    """Store S3 data source configuration."""

    aws_access_key_id: str | None = None
    """The AWS access key id. If provided, overwrite profile credentials."""

    aws_secret_access_key: pydantic.SecretStr | None = None
    """The AWS secret access key. If provided, overwrite profile credentials."""

    profile_name: str | None = None
    """A profile stored in `~/.aws/credentials`"""

    region_name: str = "us-east-1"
    """Default region when creating new connections"""

    download_limit: pydantic.PositiveInt = TIB
    """S3 datasets download quota, in bytes. Set by default to 1 TiB."""

    download_limit_do_not_change: pydantic.PositiveInt = pydantic.Field(default=20 * TIB, frozen=True)
    """A second download limit fo S3 datasets, in bytes. Set by default to 20 TiB."""

    @pydantic.field_validator("aws_secret_access_key")
    @classmethod
    def _cast_to_secret_str(cls, value) -> pydantic.SecretStr | None:
        if isinstance(value, str):
            value = pydantic.SecretStr(value)
        return value
