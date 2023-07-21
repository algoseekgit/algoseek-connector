"""General utility functions."""

import datetime
import enum
import re
from dataclasses import dataclass


class ExpirationMonthCode(enum.Enum):
    """Represent Expiration month codes for futures."""

    F = 1
    G = 2
    H = 3
    J = 4
    K = 5
    M = 6
    N = 7
    Q = 8
    U = 9
    V = 10
    X = 11
    Z = 12


@dataclass(frozen=True)
class FuturesTradingCode:
    """Storage class for futures trading code data."""

    product: str
    month: int
    year: int

    @staticmethod
    def from_str(code: str) -> "FuturesTradingCode":
        """Create a new instance from a code string."""
        if len(code) <= 2:
            msg = "Invalid code format."
            raise ValueError(msg)

        product = code[:-2]

        try:
            month_code = code[-2]
            month = ExpirationMonthCode[month_code].value
        except KeyError:
            msg = f"{code[-2]} is not a valid month code."
            raise ValueError(msg)

        try:
            year = int(code[-1])
        except ValueError:
            msg = f"{code[-1]} is not a valid year code."
            raise ValueError(msg)

        return FuturesTradingCode(product, month, year)


def yyyymmdd_str_to_date(date_str: str) -> datetime.date:
    """
    Convert a yyyymmdd str (e.g. ``"20170721"``) to a Date object.

    Parameters
    ----------
    date_str : str
        date string in `yyyymmdd` format.

    Returns
    -------
    date

    Raises
    ------
    ValueError
        If an invalid string is provided.

    Examples
    --------
    >>> import algoseek_connector as ac
    >>> ac.utils.yyyymmdd_str_to_date("20191231")
    datetime.date(2019, 12, 31)

    """
    pattern = r"(20\d{2})(\d{2})(\d{2})"
    match = re.fullmatch(pattern, date_str)
    if match is None:
        msg = f"{date_str} does not match the yyyymmdd format."
        raise ValueError(msg)

    return datetime.datetime.strptime(date_str, "%Y%m%d").date()


def iterate_date_range(start: datetime.date, end: datetime.date):
    """
    Yield date objects in the range [start:end].

    Parameters
    ----------
    start : datetime.date
    end : datetime.date

    Yields
    ------
    datetime.date

    """
    delta = datetime.timedelta(days=1)
    current = start
    while current <= end:
        yield current
        current += delta


def remove_duplicates_preserve_order(input_list: list[str]) -> list[str]:
    """Create a copy of a list with duplicates removed maintaining the order."""
    seen = set()
    return [x for x in input_list if not (x in seen or seen.add(x))]
