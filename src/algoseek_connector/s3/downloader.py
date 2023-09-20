"""Utilities to download files from S3 buckets."""

import datetime
import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Generator, Optional, Union, cast

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from .. import constants, utils

date_like = Union[datetime.date, str]


class FileDownloader:
    """Download files from S3 buckets."""

    def __init__(self, session: boto3.Session):
        self.session = session
        self.s3 = get_s3_client(session)

    def copy(self) -> "FileDownloader":
        """Create an independent copy of the current instance."""
        profile_name = self.session.profile_name
        if profile_name == "default":
            credentials = self.session.get_credentials()
            session = create_boto3_session(
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
            )
        else:
            session = create_boto3_session(profile_name=profile_name)
        return FileDownloader(session)

    def download(self, bucket_name: str, keys: list[str], download_path: Path):
        """
        Download a list of objects from a bucket.

        Parameters
        ----------
        bucket_name : str
            The bucket name.
        keys : list[str]
            The keys of object to be downloaded. If a key does not exists it is
            ignored
        download_path : pathlib.Path
            Directory to download the files.
        """
        bucket = BucketWrapper(self.s3, bucket_name)
        for key in keys:
            key_download_path = download_path / key

            # create parent directories if necessary
            parent_dir = key_download_path.parent
            parent_dir.mkdir(parents=True, exist_ok=True)

            try:
                bucket.download_file(key, key_download_path)
            except ClientError:  # ignore missing files.
                continue


class BucketWrapper:
    """A thin wrapper for boto3 Bucket objects."""

    def __init__(self, s3_client: BaseClient, bucket_name: str) -> None:
        bucket = s3_client.Bucket(bucket_name)
        if bucket.creation_date is None:
            msg = f"Bucket with name {bucket_name} not found."
            raise ValueError(msg)
        self._bucket = bucket

    def check_object_exists(self, key: str) -> bool:
        """
        Check if an object exists.

        Parameters
        ----------
        key : str
            The object key.

        Returns
        -------
        bool

        """
        try:
            self._bucket.Object(key).last_modified
            res = True
        except ClientError:
            res = False
        return res

    def delete_file(self, key: str):
        """
        Delete a file.

        If the file does not exists, no exceptions are raised.
        """
        self._bucket.Object(key).delete()

    def download_file(self, key: str, download_path: Path):
        """
        Download a file from a bucket.

        Parameters
        ----------
        key : str
            The object key.
        download_path : pathlib.Path

        Raises
        ------
        botocore.exceptions.ClientError
            If a non existent key is passed.

        """
        file_object = self._bucket.Object(key)
        file_object.download_file(download_path)

    def get_file_size(self, key: str) -> int:
        """
        Download a file from a bucket.

        Parameters
        ----------
        key : str
            The object key.
        download_path : pathlib.Path

        Raises
        ------
        botocore.exceptions.ClientError
            If a non existent key is passed.

        """
        file_object = self._bucket.Object(key)
        return file_object.content_length

    def get_object_url(self, key: str) -> str:
        """
        Get the URL of an object.

        No validations/exist checks are performed on the key.

        """
        bucket_name = self._bucket.name
        location_metadata = self._bucket.meta.client.get_bucket_location(
            Bucket=bucket_name
        )
        location = location_metadata["LocationConstraint"]
        # using virtual style style address
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#virtual-hosted-style-access
        return f"https://{bucket_name}.s3.{location}.amazonaws.com/{key}"

    def upload_file(self, key: str, upload_path: Path):
        """
        Upload a file into the bucket.

        Parameters
        ----------
        key : str
            The key of the object uploaded.
        upload_path : Path
            The path of the uploaded file.

        """
        self._bucket.upload_file(upload_path, key)


class S3KeyFilter:
    """
    Stores filter values for filtering objects in S3 buckets.

    date : str, date or tuple
    symbols : str or list[str]
    expiration_date : str, date or tuple

    """

    def __init__(
        self,
        date: Union[date_like, tuple[date_like, date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Union[date_like, tuple[date_like, date_like], None] = None,
    ):
        self.date = _normalize_date_spec(date)
        self.symbols = _normalize_symbol_spec(symbols)
        if expiration_date is None:
            self.expiration_date = None
        else:
            self.expiration_date = _normalize_date_spec(expiration_date)


class TokenType(enum.Enum):
    """Token type of S3PathToken."""

    path = 1
    separator = 2


class PlaceholderType(enum.Enum):
    """Value type associated with a placeholder."""

    none = 1
    date = 2
    symbol = 3
    expiration_date = 4
    futures = 5


class PlaceHolder(enum.Enum):
    """Placeholders values for S3 object names."""

    yyyymmdd = 1
    yyyy = 2
    sss = 3
    s = 4
    expdate = 5
    ss = 6
    ssmy = 7


@dataclass()
class S3PathToken:
    """Token for S3 object path format parsing."""

    template: str
    token_type: TokenType
    type: PlaceholderType
    placeholders: set[PlaceHolder]


class BasePrefixGenerator(ABC):
    """Base class to generate S3 object key prefixes."""

    @abstractmethod
    def create_fillers(self) -> list["BasePlaceholderFiller"]:
        """Create a list of placeholder filler."""

    def create_fill_values(
        self, template: str, placeholders: list[PlaceHolder]
    ) -> list[str]:
        """Create a list of fill values for S3 objects."""
        return [x.fill(template, placeholders) for x in self.create_fillers()]


class DatePrefixGenerator(BasePrefixGenerator):
    """Create prefixes for dates."""

    def __init__(self, start_date: datetime.date, end_date: datetime.date):
        self._start_date = start_date
        self._end_date = end_date

    def create_fillers(self) -> list["DatePlaceholderFiller"]:
        """Create a list of filler objects."""
        start = self._start_date
        end = self._end_date
        return [DatePlaceholderFiller(x) for x in utils.iterate_date_range(start, end)]


class SymbolPrefixGenerator(BasePrefixGenerator):
    """
    Create prefixes for tickers.

    Parameters
    ----------
    symbols : str or list[str]
        Tickers used to generate prefixes.

    """

    def __init__(self, symbols: list[str]) -> None:
        self._symbols = symbols

    def create_fillers(self) -> list["SymbolPlaceholderFiller"]:
        """Create a list of filler objects."""
        return [SymbolPlaceholderFiller(x) for x in self._symbols]


class FuturesPrefixGenerator(SymbolPrefixGenerator, DatePrefixGenerator):
    """Create prefixes for futures codes."""

    def __init__(
        self,
        tickers: list[str],
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> None:
        SymbolPrefixGenerator.__init__(self, tickers)
        DatePrefixGenerator.__init__(self, start_date, end_date)

    def create_fillers(self) -> list["FuturesPlaceholderFiller"]:
        """Create a list of filler objects."""
        fillers = list()
        current = self._start_date
        while current <= self._end_date:
            for symbol in self._symbols:
                f = FuturesPlaceholderFiller(symbol, current)
                fillers.append(f)
            dy, dm = divmod(current.month, 12)
            next_year = current.year + dy
            next_month = dm + 1
            current = datetime.date(next_year, next_month, current.day)
        return fillers


class BasePlaceholderFiller(ABC):
    """

    Base class that fills placeholder values in a S3 object key.

    Methods to create placeholder fillers must start with `get_`.

    See DatePlaceholderFiller for an example.

    """

    def create_fill_values(self, placeholders: list[PlaceHolder]) -> dict[str, str]:
        """Create values to fill a string template."""
        placeholder_to_value = dict()
        for p in placeholders:
            if isinstance(p, PlaceHolder):
                name = p.name
                placeholder_func = getattr(self, f"get_{name}")
                placeholder_to_value[name] = placeholder_func()
            else:
                raise ValueError(f"{p} is not a Placeholder instance.")
        return placeholder_to_value

    def fill(self, template: str, placeholders: list[PlaceHolder]) -> str:
        """Replace the placeholder values in the template to generate a prefix string."""
        placeholder_to_value = self.create_fill_values(placeholders)
        return template.format(**placeholder_to_value)

    @classmethod
    def list_available_placeholders(cls) -> list[str]:
        """List all available placeholders."""
        return [x.split("_")[-1] for x in cls.__dict__ if x.startswith("get_")]


class DatePlaceholderFiller(BasePlaceholderFiller):
    """Fills placeholder values associated with dates."""

    def __init__(self, date: datetime.date) -> None:
        self._date = date

    def get_yyyy(self) -> str:
        """Get the year in format yyyy."""
        return str(self._date.year)

    def get_yyyymmdd(self) -> str:
        """Get a timestamp in format yyyymmdd."""
        return self._date.strftime("%Y%m%d")


class SymbolPlaceholderFiller(BasePlaceholderFiller):
    """Fills placeholder values associated with tickers."""

    def __init__(self, ticker: str):
        self._ticker = ticker

    def get_s(self) -> str:
        """Get the first letter of the ticker."""
        return self._ticker[0]

    def get_sss(self) -> str:
        """Get the ticker name."""
        return self._ticker


class ExpirationDatePlaceholderFiller(BasePlaceholderFiller):
    """Fills placeholder values associated with dates."""

    def __init__(self, date: datetime.date) -> None:
        self._date = date

    def get_expdate(self) -> str:
        """Get a timestamp in format yyyymmdd."""
        return self._date.strftime("%Y%m%d")


class FuturesPlaceholderFiller(BasePlaceholderFiller):
    """Fill placeholder values associated with futures codes."""

    def __init__(self, symbol: str, date: datetime.date):
        self._symbol = symbol
        self._date = date

    def get_ss(self) -> str:
        """Get the base future symbol."""
        return self._symbol

    def get_my(self) -> str:
        """Get the month code and last digit of the year of the expiration date."""
        month_code = utils.ExpirationMonthCode(self._date.month).name
        last_digit_year = str(self._date.year)[-1]
        return f"{month_code}{last_digit_year}"

    def get_ssmy(self) -> str:
        """Get the future code."""
        return self.get_ss() + self.get_my()


def create_boto3_session(
    profile_name: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    **kwargs,
) -> boto3.Session:
    """
    Create a Session instance.

    Parameters
    ----------
    profile_name : str or None, default=None
        If a profile name is specified, the access key and secret key are
        retrieved from the file `~/.aws/credentials`, and the parameters
        `aws_access_key_id` and `aws_secret_access_key` are ignored. If
        ``None``, this field is ignored.
    aws_access_key_id : str or None, default=None
        The AWS access key associated with an IAM user or role.
    aws_secret_access_key : str or None, default=None
        Thee secret key associated with the access key.
    **kwargs :
        Optional parameters passed to boto3.Session.

    Returns
    -------
    boto3.Session

    Raises
    ------
    TypeError
        If a secret key is provided but not user is provided.
    botocore.exception.ClientError
        If the credentials are not valid.

    """
    if profile_name is None:
        session = boto3.Session(aws_access_key_id, aws_secret_access_key, **kwargs)
    else:
        session = boto3.Session(profile_name=profile_name, **kwargs)
    _validate_session(session)
    return session


def create_key_to_size_dictionary(
    bucket: BucketWrapper, path_format: str, filters: S3KeyFilter
) -> dict[str, int]:
    """
    Create a dict of object keys to object size.

    Parameters
    ----------
    bucket : BucketWrapper
    bucket_name : str
    path_format : str
        The format of the object keys in the bucket.
    filters : S3KeyFilter
        Filters for object key names.

    Returns
    -------
    dict[str, int]

    """
    key_to_size = dict()
    for key in _generate_object_keys(path_format, filters):
        try:
            key_to_size[key] = bucket.get_file_size(key)
        except ClientError:
            continue
    return key_to_size


def _split_into_even_size(keys_to_size: dict[str, int], n: int) -> list[list[str]]:
    """Split keys into n even-sized list of keys."""
    even_sized_groups = list()
    max_chunk_size = sum(keys_to_size.values()) // n
    current_chunk_size = 0
    current_group = list()
    for k, size in keys_to_size.items():
        current_group.append(k)
        current_chunk_size += size
        if current_chunk_size > max_chunk_size:
            current_group = list()
    return even_sized_groups


def _generate_object_keys(
    path_format: str, filters: S3KeyFilter
) -> Generator[str, None, None]:
    """
    Yield object keys compatible with the filters provided.

    Parameters
    ----------
    path_format : str
        The path format of objects in the bucket.
    filters : S3KeyFilter
        Filters for symbol, date and expiration date.

    Yields
    ------
    str
        An object key name.
    """
    prefix_separator = "/"
    name_separator = "."
    tokens = _tokenize_path_format(path_format, prefix_separator, name_separator)
    prefixes = list()
    for t in tokens:
        prefix_generator = _get_prefix_generator(t, filters)
        p = prefix_generator.create_fill_values(t.template, list(t.placeholders))
        prefixes.append(p)

    for key_parts in product(*prefixes):
        yield "".join(key_parts)


def _tokenize_path_format(
    path_format: str, prefix_sep: str, name_sep: str
) -> list[S3PathToken]:
    """Convert path_format specification into a list of tokens."""
    parts = _split_path_format(path_format, prefix_sep, name_sep)
    tokens = _create_tokens(parts, prefix_sep, name_sep)
    tokens = _merge_tokens(tokens)
    return tokens


def _get_prefix_generator(
    token: "S3PathToken", filters: S3KeyFilter
) -> "BasePrefixGenerator":
    """Create a PrefixGenerator instance."""
    if token.type == PlaceholderType.date:
        prefix_generator = DatePrefixGenerator(*filters.date)
    elif token.type == PlaceholderType.symbol:
        prefix_generator = SymbolPrefixGenerator(filters.symbols)
    else:
        if filters.expiration_date is None:
            msg = (
                "Expiration date must be specified to download data from this dataset."
            )
            raise ValueError(msg)
        prefix_generator = FuturesPrefixGenerator(
            filters.symbols, *filters.expiration_date
        )
    return prefix_generator


def _split_path_format(path_format: str, prefix_sep: str, name_sep: str) -> list[str]:
    """Split a path format into parts of prefixes, separators and names."""
    prefix_parts = path_format.split(prefix_sep)
    name = prefix_parts.pop()

    parts = list()
    for part in prefix_parts:
        parts.append(part)
        parts.append(prefix_sep)

    for part in name.split(name_sep):
        parts.append(part)
        parts.append(name_sep)

    parts.pop()  # remove separator token added at the end by _create_tokens
    return parts


def _create_tokens(
    parts: list[str], prefix_sep: str, name_sep: str
) -> list[S3PathToken]:
    tokens = list()
    last = S3PathToken("", TokenType.path, PlaceholderType.none, set())
    tokens.append(last)
    for part in parts:
        try:
            placeholder = PlaceHolder[part]
            placeholder_type = _get_placeholder_type(placeholder)
            template = f"{{{part}}}"
        except KeyError:
            placeholder = None
            placeholder_type = PlaceholderType.none
            template = part
        separators = [prefix_sep, name_sep]
        token_type = TokenType.separator if part in separators else TokenType.path
        current = S3PathToken(template, token_type, placeholder_type, set())
        if placeholder is not None:
            current.placeholders.add(placeholder)
        tokens.append(current)
    return tokens


def _get_placeholder_type(placeholder: PlaceHolder) -> PlaceholderType:
    if placeholder in [PlaceHolder.s, PlaceHolder.sss]:
        t = PlaceholderType.symbol
    elif placeholder in [PlaceHolder.yyyy, PlaceHolder.yyyymmdd]:
        t = PlaceholderType.date
    elif placeholder == PlaceHolder.expdate:
        t = PlaceholderType.expiration_date
    else:
        t = PlaceholderType.futures
    return t


def _merge_tokens(tokens: list[S3PathToken]) -> list[S3PathToken]:
    """Merge consecutive tokens of the same type into a single token template."""
    merged = list()
    last = S3PathToken("", TokenType.path, PlaceholderType.none, set())
    merged.append(last)
    for current in tokens:
        if last.type is PlaceholderType.none or last.type == current.type:
            last.template += current.template
            last.type = current.type
            last.placeholders.update(current.placeholders)
        elif current.type == PlaceholderType.none:
            last.template += current.template
        elif current.token_type == TokenType.separator:
            last.template += current.template
        else:
            merged.append(current)
            last = current
    return merged


def get_bucket_name(
    bucket_format: str, start_date: datetime.date, end_date: datetime.date
) -> str:
    """Get the bucket name from a bucket format template."""
    # TODO: Currently hardcoded to work with date intervals from a single year.
    prefix_sep = "-"
    name_sep = "."
    tokens = _tokenize_path_format(bucket_format, prefix_sep, name_sep)
    if start_date.year != end_date.year:
        start_timestamp = start_date.strftime("%Y%m%d")
        end_timestamp = end_date.strftime("%Y%m%d")
        date_range = (start_timestamp, end_timestamp)
        msg = f"Date ranges must start and end on the same year. Got {date_range}."
        raise ValueError(msg)
    else:
        year = start_date.year

    fill = {"yyyy": year}
    return "".join([x.template.format(**fill) for x in tokens])


def _normalize_date_spec(
    date: Union[date_like, tuple[date_like, date_like]]
) -> tuple[datetime.date, datetime.date]:
    if isinstance(date, str):
        start_date = end_date = _normalize_date(date)
    elif isinstance(date, datetime.date):
        start_date = end_date = date
    elif isinstance(date, tuple):
        start_date = _normalize_date(date[0])
        end_date = _normalize_date(date[1])
    else:
        msg = "{values} is not a valid specification for date filter."
        raise ValueError(msg)
    if start_date > end_date:
        msg = "Invalid date specification: start date should be earlier than end date."
        raise ValueError(msg)
    return start_date, end_date


def _normalize_symbol_spec(symbols: Union[str, list[str]]) -> list[str]:
    if isinstance(symbols, str):
        symbols_list = [symbols]
    elif isinstance(symbols, list) and all(isinstance(x, str) for x in symbols):
        symbols_list = symbols
    else:
        msg = f"{symbols} is not a valid specification for Tickers filter."
        raise ValueError(msg)
    return symbols_list


def _normalize_date(date):
    if isinstance(date, datetime.date):
        normalized = date
    elif isinstance(date, str):
        normalized = utils.yyyymmdd_str_to_date(date)
    else:
        msg = f"{date} is not a supported format for date."
        raise ValueError(msg)
    return normalized


def get_s3_client(session: boto3.Session) -> BaseClient:
    """Create a S3 client."""
    return cast(BaseClient, session.resource(constants.S3))


def _validate_session(session: boto3.Session):
    """
    Check if the credentials passed to the session are valid.

    botocore.exceptions.ClientError is raised if the credentials are not valid.
    """
    session.client("sts").get_caller_identity()
