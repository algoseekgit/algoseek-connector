import datetime

import pytest


@pytest.mark.parametrize(
    "date,expected",
    [
        (datetime.date(1999, 10, 1), "19991001"),
        (datetime.date(2008, 5, 14), "20080514"),
        (datetime.date(2010, 11, 29), "20101129"),
    ],
)
def test_parse_date_to_yyyymmdd(date: datetime.date, expected: str):
    # test if "%Y%m%d" is the correct spec to create strings with yyyymmdd format
    actual = date.strftime("%Y%m%d")
    assert actual == expected
