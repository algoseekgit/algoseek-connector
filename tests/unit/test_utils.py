import pytest
from algoseek_connector import utils


@pytest.mark.parametrize(
    "date_str,year,month,day",
    [
        ("20220303", 2022, 3, 3),
        ("20070101", 2007, 1, 1),
        ("20191231", 2019, 12, 31),
        ("19990303", 1999, 3, 3),
    ],
)
def test_yyyymmdd_str_to_date(date_str: str, year: int, month: int, day: int):
    date = utils.yyyymmdd_str_to_date(date_str)
    assert date.year == year
    assert date.month == month
    assert date.day == day


@pytest.mark.parametrize("date_str", ["20071401", "20191232", "-99990101"])
def test_yyyymmdd_str_to_date_invalid_date(date_str: str):
    with pytest.raises(ValueError):
        utils.yyyymmdd_str_to_date(date_str)


@pytest.mark.parametrize(
    "lst,expected",
    [
        (["a", "b", "c", "b"], ["a", "b", "c"]),
        (["a", "b", "b", "c", "d", "c"], ["a", "b", "c", "d"]),
        ([], []),
    ],
)
def test_remove_duplicates_preserve_order(lst: list[str], expected: list[str]):
    actual = utils.remove_duplicates_preserve_order(lst)
    assert actual == expected
