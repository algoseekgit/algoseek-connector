"""Algoseek connector data models."""

from __future__ import annotations

import enum

import pydantic

TIB = 1024**4  # 1 tebibyte


class DataSourceType(str, enum.Enum):
    """Available data source types."""

    ARDADB = "ArdaDB"
    """The ArdaDB data source."""

    S3 = "S3"
    """The S3 data source."""


class DatasetAPIConfiguration(pydantic.BaseModel):
    """Store dataset API configuration."""

    url: str = "https://datasets-metadata.algoseek.com/api/v2"
    """The API base URL"""

    headers: dict[str, str] | None = None
    """Headers to include in all requests."""

    email: str | None = None
    """the email to request an access token."""

    password: str | None = None
    """the password to request an access token."""

    @pydantic.model_validator(mode="after")
    def _set_credentials(self):
        """Set API credentials. Defaults are set here to avoid showing default credentials in API docs."""
        self.email = "connector-lib@algoseek.com" if self.email is None else self.email
        self.password = "57xB_d69U_Mqgq_uzrP" if self.password is None else self.password
        return self


class ArdaDBConfiguration(pydantic.BaseModel):
    """Store ArdaDB data source configuration."""

    host: str = "0.0.0.0"
    """The ArdaDB host Address"""

    port: pydantic.PositiveInt = 8123
    """The ArdaDB connection port"""

    user: str = ""
    """The ArdaDB user name"""

    password: str = ""
    """The ArdaDB password"""

    extra: dict = dict()
    """Optional arguments passed to clickhouse_connect.get_client. See
    `here <https://clickhouse.com/docs/en/integrations/python#clickhouse-connect-driver-api>`_
    for a description of the parameters that are accepted.
    """


class S3Configuration(pydantic.BaseModel):
    """Store S3 data source configuration."""

    aws_access_key_id: str | None = None
    """The AWS access key id. If provided, overwrite profile credentials."""

    aws_secret_access_key: str | None = None
    """The AWS secret access key. If provided, overwrite profile credentials."""

    profile_name: str | None = None
    """A profile stored in `~/.aws/credentials`"""

    region_name: str = "us-east-1"
    """Default region when creating new connections"""

    download_limit: pydantic.PositiveInt = TIB
    """S3 datasets download quota, in bytes. Set by default to 1 TiB."""

    download_limit_do_not_change: pydantic.PositiveInt = pydantic.Field(default=20 * TIB, frozen=True)
    """A second download limit fo S3 datasets, in bytes. Set by default to 20 TiB."""
