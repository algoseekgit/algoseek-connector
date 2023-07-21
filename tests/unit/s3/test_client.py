import datetime

import pytest

from algoseek_connector.s3 import client


@pytest.mark.parametrize(
    "path_format,expected_template,expected_placeholders",
    [
        (
            "yyyymmdd/ss/ssmy.csv.gz",
            "{yyyymmdd}/{ss}/{ssmy}.csv.gz",
            ["yyyymmdd", "ss", "ssmy"],
        ),
        (
            "yyyymmdd/s/sss/sss.expdate.csv.gz",
            "{yyyymmdd}/{s}/{sss}/{sss}.{expdate}.csv.gz",
            ["yyyymmdd", "s", "sss", "expdate"],
        ),
        (
            "futures_security_master.csv",
            "futures_security_master.csv",
            [],
        ),
        ("yyyymmdd.csv", "{yyyymmdd}.csv", ["yyyymmdd"]),
        ("index/yyyymmdd.csv", "index/{yyyymmdd}.csv", ["yyyymmdd"]),
        (
            "yyyymmdd/s/sss.csv.gz",
            "{yyyymmdd}/{s}/{sss}.csv.gz",
            ["yyyymmdd", "s", "sss"],
        ),
    ],
)
def test_parse_bucket_path_format(
    path_format: str, expected_template: str, expected_placeholders: list[str]
):
    actual_template, actual_placeholders = client.create_objects_path_template(
        path_format
    )
    assert actual_template == expected_template
    assert actual_placeholders == expected_placeholders


def test__make_date_prefixes_str_tuple():
    start = "20220329"
    end = "20220403"
    expected = ["20220329", "20220330", "20220331", "20220401", "20220402", "20220403"]
    actual = client._make_list_of_date_prefixes((start, end))
    assert actual == expected


def test__make_date_prefixes_single_str():
    date = "20220329"
    expected = ["20220329"]
    actual = client._make_list_of_date_prefixes(date)
    assert actual == expected


def test__make_date_prefixes_date_tuple():
    start = datetime.date(2022, 3, 29)
    end = datetime.date(2022, 4, 3)
    expected = ["20220329", "20220330", "20220331", "20220401", "20220402", "20220403"]
    actual = client._make_list_of_date_prefixes((start, end))
    assert actual == expected


def test__make_date_prefixes_single_date():
    date = datetime.date(2022, 3, 29)
    expected = ["20220329"]
    actual = client._make_list_of_date_prefixes(date)
    assert actual == expected


def test__make_date_prefixes_tuple_start_date_greater_than_end_date_returns_empty():
    start = datetime.date(2022, 4, 29)
    end = datetime.date(2022, 4, 3)
    actual = client._make_list_of_date_prefixes((start, end))
    assert len(actual) == 0
