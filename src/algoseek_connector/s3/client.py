"""Client protocol for S3 data."""

import datetime

from .. import utils


class PlaceHolders:
    """Container class for S3 Objects name placeholders."""

    DATE = "yyyymmdd"
    YEAR = "yyyy"
    TICKER = "sss"
    TICKER_START = "s"
    EXPIRATION_DATE = "expdate"
    PRODUCT_CODE = "ss"
    TRADE_CODE = "ssmy"

    # TODO: make a class iterator to avoid repetition
    ALL = {"yyyymmdd", "yyyy", "ss", "ssmy", "s", "sss", "expdate"}


class S3DataFetcher:
    """Fetch data from S3 buckets."""

    def __init__(self, bucket_path_format: str, object_path_format: str):
        prefixes, name = _split_object_path_into_prefixes(object_path_format)
        name_template, name_placeholders = _make_name_template(name)
        prefix_template, prefix_placeholders = _make_prefix_template(prefixes)
        self.name = name_template
        self.prefix = prefix_template
        self.name_placeholders = name_placeholders
        self.prefix_placeholders = prefix_placeholders

    def _create_prefixes(self, **kwargs):
        prefixes = dict()
        for placeholder, values in kwargs.items():
            if placeholder == PlaceHolders.DATE:
                prefixes[placeholder] = _make_list_of_date_prefixes(values)
            elif placeholder == "s":
                pass


def _make_list_of_tickers(values) -> list[str]:
    if isinstance(values, str):
        tickers = [values]
    elif isinstance(values, list) and all(isinstance(x, str) for x in values):
        tickers = values
    else:
        msg = f"{values} is not a valid specification for Tickers filter."
        raise ValueError(msg)
    return tickers


def _make_list_of_date_prefixes(values) -> list[str]:
    if isinstance(values, tuple):
        start_date = _normalize_date(values[0])
        end_date = _normalize_date(values[1])
    elif isinstance(values, (datetime.date, str)):
        start_date = _normalize_date(values)
        end_date = start_date
    else:
        msg = "{values} is not a valid specification for date filter."
        raise ValueError(msg)
    date_range = utils.iterate_date_range(start_date, end_date)
    return list(x.strftime("%Y%m%d") for x in date_range)


def _normalize_date(date):
    if isinstance(date, datetime.date):
        normalized = date
    elif isinstance(date, str):
        normalized = utils.yyyymmdd_str_to_date(date)
    else:
        msg = f"{date} is not a supported format for date."
        raise ValueError(msg)
    return normalized


def create_objects_path_template(path: str) -> tuple[str, list[str]]:
    """
    Create a template string for object path creation in a S3 bucket.

    Parameters
    ----------
    path : str
        Path format for the Bucket group.

    Returns
    -------
    tuple[str, list[str]]
        The string with the path template and a list with the placeholder names
        used.

    """
    prefixes, name = _split_object_path_into_prefixes(path)
    name_template, name_placeholders = _make_name_template(name)
    prefix_template, prefix_placeholders = _make_prefix_template(prefixes)
    if prefix_template:
        template = f"{prefix_template}/{name_template}"
    else:
        template = name_template

    placeholders = prefix_placeholders + name_placeholders
    placeholders = utils.remove_duplicates_preserve_order(placeholders)
    return template, placeholders


def _split_object_path_into_prefixes(path: str) -> tuple[list[str], str]:
    parts = path.split("/")
    has_prefixes = len(parts) > 1
    if has_prefixes:
        prefixes = parts[:-1]
        name = parts[-1]
    else:
        prefixes = list()
        name = parts[0]
    return prefixes, name


def _make_name_template(name: str) -> tuple[str, list[str]]:
    parts = name.split(".")

    if parts[-1] in ["zip", "gz"]:
        extension = ".".join(parts[-2:])
        name_parts = parts[:-2]
    else:
        extension = parts[-1]
        name_parts = parts[:-1]

    name_placeholders = [x for x in name_parts if x in PlaceHolders.ALL]
    name_parts = [f"{{{x}}}" if x in PlaceHolders.ALL else x for x in name_parts]
    name_template = ".".join(name_parts) + f".{extension}"
    return name_template, name_placeholders


def _make_prefix_template(prefixes: list[str]) -> tuple[str, list[str]]:
    prefixes_placeholders = [x for x in prefixes if x in PlaceHolders.ALL]
    prefixes = [f"{{{x}}}" if x in PlaceHolders.ALL else x for x in prefixes]
    prefix_template = "/".join(prefixes)
    return prefix_template, prefixes_placeholders
