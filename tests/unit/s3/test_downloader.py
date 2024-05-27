import datetime
from pathlib import Path
from typing import cast

import pytest
from algoseek_connector.s3 import downloader
from algoseek_connector.s3.downloader import PlaceHolder


def test_DatePlaceholderFiller_list_available_placeholders():
    placeholders = downloader.DatePlaceholderFiller.list_available_placeholders()
    assert len(placeholders) == 2
    assert downloader.PlaceHolder.yyyy.name in placeholders
    assert downloader.PlaceHolder.yyyymmdd.name in placeholders


def test_DatePlaceholderFiller_create_fill_values_yyyy():
    today = datetime.date.today()
    filler = downloader.DatePlaceholderFiller(today)
    placeholder = PlaceHolder.yyyy
    expected = {placeholder.name: str(today.year)}
    actual = filler.create_fill_values([placeholder])
    assert actual == expected


def test_DatePlaceholderFiller_create_fill_values_yyyymmdd():
    today = datetime.date.today()
    filler = downloader.DatePlaceholderFiller(today)
    placeholder = PlaceHolder.yyyymmdd
    expected = {placeholder.name: today.strftime("%Y%m%d")}
    actual = filler.create_fill_values([placeholder])
    assert actual == expected


def test_DatePlaceholderFiller_create_fill_values_yyyymmdd_and_yyyy():
    today = datetime.date.today()
    filler = downloader.DatePlaceholderFiller(today)
    expected = {
        PlaceHolder.yyyymmdd.name: today.strftime("%Y%m%d"),
        PlaceHolder.yyyy.name: str(today.year),
    }
    placeholders = [PlaceHolder.yyyy, PlaceHolder.yyyymmdd]
    actual = filler.create_fill_values(placeholders)
    assert actual == expected


def test_DatePlaceholderFiller_create_fill_values_invalid_placeholder():
    today = datetime.date.today()
    filler = downloader.DatePlaceholderFiller(today)
    with pytest.raises(ValueError):
        placeholder = cast(PlaceHolder, "invalid")
        filler.create_fill_values([placeholder])


def test_DatePlaceholderFiller_fill():
    date = datetime.date(2023, 7, 29)
    filler = downloader.DatePlaceholderFiller(date)
    template = "{yyyy}-myfile-{yyyymmdd}"
    expected = "2023-myfile-20230729"
    placeholders = [PlaceHolder.yyyy, PlaceHolder.yyyymmdd]
    actual = filler.fill(template, placeholders)
    assert actual == expected


def test_TickerPlaceholderFiller_list_available_placeholders():
    placeholders = downloader.SymbolPlaceholderFiller.list_available_placeholders()
    assert len(placeholders) == 2
    assert downloader.PlaceHolder.s.name in placeholders
    assert downloader.PlaceHolder.sss.name in placeholders


def test_SymbolPlaceholderFIller_create_fill_values_s():
    symbol = "ABC"
    filler = downloader.SymbolPlaceholderFiller(symbol)
    placeholder = PlaceHolder.s
    expected = {placeholder.name: symbol[0]}
    actual = filler.create_fill_values([placeholder])
    assert actual == expected


def test_SymbolPlaceholderFIller_create_fill_values_sss():
    symbol = "ABC"
    filler = downloader.SymbolPlaceholderFiller(symbol)
    placeholder = PlaceHolder.sss
    expected = {placeholder.name: symbol}
    actual = filler.create_fill_values([placeholder])
    assert actual == expected


def test_SymbolPlaceholderFiller_create_fill_values_s_and_sss():
    symbol = "ABC"
    filler = downloader.SymbolPlaceholderFiller(symbol)
    expected = {PlaceHolder.sss.name: symbol, PlaceHolder.s.name: symbol[0]}
    placeholders = [PlaceHolder.s, PlaceHolder.sss]
    actual = filler.create_fill_values(placeholders)
    assert actual == expected


def test_SymbolPlaceholderFiller_create_fill_values_invalid_placeholder():
    symbol = "ABC"
    filler = downloader.SymbolPlaceholderFiller(symbol)
    with pytest.raises(ValueError):
        placeholder = cast(downloader.PlaceHolder, "invalid")
        filler.create_fill_values([placeholder])


def test_SymbolPlaceholderFiller_fill():
    ticker = "ABC"
    filler = downloader.SymbolPlaceholderFiller(ticker)
    template = "my-bucket-root/{s}/{sss}"
    placeholders = [PlaceHolder.s, PlaceHolder.sss]
    actual = filler.fill(template, placeholders)
    expected = "my-bucket-root/A/ABC"
    assert actual == expected


def test_DatePrefixGenerator_single_date():
    start_date = end_date = datetime.date(2023, 7, 29)
    prefix_generator = downloader.DatePrefixGenerator(start_date, end_date)

    template = "my-bucket-root/{yyyymmdd}"
    placeholders = [PlaceHolder.yyyymmdd]
    actual = prefix_generator.create_fill_values(template, placeholders)
    expected = ["my-bucket-root/20230729"]
    assert actual == expected


def test_DatePrefixGenerator_date_range():
    start_date = datetime.date(2023, 7, 29)
    end_date = datetime.date(2023, 8, 1)
    prefix_generator = downloader.DatePrefixGenerator(start_date, end_date)

    template = "my-bucket-root/{yyyymmdd}"
    placeholders = [PlaceHolder.yyyymmdd]
    actual = prefix_generator.create_fill_values(template, placeholders)
    expected = [
        "my-bucket-root/20230729",
        "my-bucket-root/20230730",
        "my-bucket-root/20230731",
        "my-bucket-root/20230801",
    ]
    assert actual == expected


def test_SymbolPrefixGenerator_single_ticker():
    symbols = ["ABC"]
    prefix_generator = downloader.SymbolPrefixGenerator(symbols)

    template = "my-bucket-root/{s}/{sss}"
    placeholders = [PlaceHolder.s, PlaceHolder.sss]
    actual = prefix_generator.create_fill_values(template, placeholders)
    expected = ["my-bucket-root/A/ABC"]
    assert actual == expected


def test_TickerPrefixGenerator_multiple_tickers():
    symbols = ["ABC", "ABB", "AGG", "EBB"]
    prefix_generator = downloader.SymbolPrefixGenerator(symbols)

    template = "my-bucket-root/{s}/{sss}"
    placeholders = [PlaceHolder.s, PlaceHolder.sss]
    actual = prefix_generator.create_fill_values(template, placeholders)
    expected = [
        "my-bucket-root/A/ABC",
        "my-bucket-root/A/ABB",
        "my-bucket-root/A/AGG",
        "my-bucket-root/E/EBB",
    ]
    assert actual == expected


@pytest.mark.parametrize(
    "path_format,expected",
    [
        (
            "yyyymmdd/s/sss.csv.gz",
            ["yyyymmdd", "/", "s", "/", "sss", ".", "csv", ".", "gz"],
        ),
        ("xx/xxxxx.csv", ["xx", "/", "xxxxx", ".", "csv"]),
        (
            "yyyymmdd/ss/ssmy.csv.gz",
            ["yyyymmdd", "/", "ss", "/", "ssmy", ".", "csv", ".", "gz"],
        ),
        ("so_detailed.csv", ["so_detailed", ".", "csv"]),
    ],
)
def test_split_path_format(path_format, expected):
    actual = downloader._split_path_format(path_format, "/", ".")
    assert actual == expected


@pytest.mark.parametrize(
    "path_format,expected_template_parts,expected_placeholders",
    [
        (
            "yyyymmdd/s/sss.csv.gz",
            ["{yyyymmdd}/", "{s}/{sss}.csv.gz"],
            [{PlaceHolder.yyyymmdd}, {PlaceHolder.s, PlaceHolder.sss}],
        ),
        ("so_detailed.csv", ["so_detailed.csv"], [set()]),
    ],
)
def test_tokenize_path_format(path_format, expected_template_parts, expected_placeholders):
    prefix_sep = "/"
    name_sep = "."
    actual = downloader._tokenize_path_format(path_format, prefix_sep, name_sep)
    for t, template_str, placeholders in zip(actual, expected_template_parts, expected_placeholders):
        assert t.template == template_str
        assert t.placeholders == placeholders


def test_S3KeyFilter_using_single_date_str():
    year, month, day = 2023, 8, 1
    date = f"{year}{month:02d}{day:02d}"
    expected_start_date = datetime.date(year, month, day)
    symbols = ["ABC", "CDE"]
    key_filter = downloader.S3KeyFilter(date, symbols)
    start_date, end_date = key_filter.date
    assert start_date == end_date
    assert start_date == expected_start_date


def test_S3KeyFilter_using_single_date():
    year, month, day = 2023, 8, 1
    expected_start_date = datetime.date(year, month, day)
    symbols = ["ABC", "CDE"]
    key_filter = downloader.S3KeyFilter(expected_start_date, symbols)
    start_date, end_date = key_filter.date
    assert expected_start_date == start_date
    assert start_date == end_date


def test_S3KeyFilter_using_date_str_tuple():
    start_year, start_month, start_day = 2023, 8, 1
    end_year, end_month, end_day = 2023, 8, 1
    expected_start_date = datetime.date(start_year, start_month, start_day)
    expected_end_date = datetime.date(end_year, end_month, end_day)
    start_date_str = f"{start_year}{start_month:02d}{start_day:02d}"
    end_date_str = f"{end_year}{end_month:02d}{end_day:02d}"
    symbols = ["ABC", "CDE"]
    key_filter = downloader.S3KeyFilter((start_date_str, end_date_str), symbols)
    actual_start_date, actual_end_date = key_filter.date
    assert actual_start_date == expected_start_date
    assert actual_end_date == expected_end_date


def test_S3KeyFilter_using_date_tuple():
    start_year, start_month, start_day = 2023, 8, 1
    end_year, end_month, end_day = 2023, 8, 1
    expected_start_date = datetime.date(start_year, start_month, start_day)
    expected_end_date = datetime.date(end_year, end_month, end_day)
    symbols = ["ABC", "CDE"]
    date = (expected_start_date, expected_end_date)
    key_filter = downloader.S3KeyFilter(date, symbols)
    actual_start_date, actual_end_date = key_filter.date
    assert actual_start_date == expected_start_date
    assert actual_end_date == expected_end_date


def test_S3KeyFilter_invalid_date_range():
    start_year, start_month, start_day = 2023, 8, 1
    end_year, end_month, end_day = 2023, 7, 1
    expected_start_date = datetime.date(start_year, start_month, start_day)
    expected_end_date = datetime.date(end_year, end_month, end_day)
    symbols = ["ABC", "CDE"]
    date = (expected_start_date, expected_end_date)
    with pytest.raises(ValueError):
        downloader.S3KeyFilter(date, symbols)


def test_S3KeyFilter_using_single_symbol_str():
    date = "20230801"
    symbol = "ABC"
    expected_symbols = [symbol]
    key_filter = downloader.S3KeyFilter(date, symbol)
    actual_symbols = key_filter.symbols
    assert actual_symbols == expected_symbols


def test_S3KeyFilter_using_multiple_symbols_str():
    date = "20230801"
    expected_symbols = ["ABC", "CDE", "FGH"]
    key_filter = downloader.S3KeyFilter(date, expected_symbols)
    actual_symbols = key_filter.symbols
    assert actual_symbols == expected_symbols


def test_S3KeyFilter_expiration_date_is_none():
    date = "20230801"
    expected_symbols = ["ABC", "CDE", "FGH"]
    key_filter = downloader.S3KeyFilter(date, expected_symbols)
    assert key_filter.expiration_date is None


def test_S3KeyFilter_expiration_date_single_expiration_date():
    date = "20230701"
    year, month, day = 2023, 8, 1
    expiration_date_str = f"{year}{month:02d}{day:02d}"
    expected_expiration_date = datetime.date(year, month, day)
    symbols = ["ABC", "CDE"]
    key_filter = downloader.S3KeyFilter(date, symbols, expiration_date=expiration_date_str)
    start_expiration_date, end_expiration_date = cast(tuple, key_filter.expiration_date)
    assert start_expiration_date == end_expiration_date
    assert start_expiration_date == expected_expiration_date


def test_S3KeyFilter_expiration_date_expiration_date_range():
    date = "20230701"
    start_year, start_month, start_day = 2023, 8, 1
    start_expiration_date_str = f"{start_year}{start_month:02d}{start_day:02d}"
    end_year, end_month, end_day = 2023, 8, 10
    end_expiration_date_str = f"{end_year}{end_month:02d}{end_day:02d}"
    expected_start_expiration_date = datetime.date(start_year, start_month, start_day)
    expected_end_expiration_date = datetime.date(end_year, end_month, end_day)
    expiration_date_str = (start_expiration_date_str, end_expiration_date_str)
    symbols = ["ABC", "CDE"]
    key_filter = downloader.S3KeyFilter(date, symbols, expiration_date=expiration_date_str)
    start_expiration_date, end_expiration_date = cast(tuple, key_filter.expiration_date)
    assert start_expiration_date == expected_start_expiration_date
    assert end_expiration_date == expected_end_expiration_date


def test_generate_object_keys_equity_data():
    path_format = "yyyymmdd/s/sss.csv.gz"
    symbols = ["ABC", "DEF"]
    start_date = "20230729"
    end_date = "20230801"
    filters = downloader.S3KeyFilter(symbols=symbols, date=(start_date, end_date))
    expected = {
        "20230729/A/ABC.csv.gz",
        "20230730/A/ABC.csv.gz",
        "20230731/A/ABC.csv.gz",
        "20230801/A/ABC.csv.gz",
        "20230729/D/DEF.csv.gz",
        "20230730/D/DEF.csv.gz",
        "20230731/D/DEF.csv.gz",
        "20230801/D/DEF.csv.gz",
    }
    actual = set(downloader._generate_object_keys(path_format, filters))
    assert actual == expected


def test_generate_object_keys_futures_data():
    path_format = "yyyymmdd/ss/ssmy.csv.gz"
    symbols = ["AB", "DE"]
    start_date = "20230729"
    end_date = "20230801"
    expiration_date_start = "20240301"
    expiration_date_end = "20240401"
    filters = downloader.S3KeyFilter(
        symbols=symbols,
        date=(start_date, end_date),
        expiration_date=(expiration_date_start, expiration_date_end),
    )
    expected = {
        "20230729/AB/ABH4.csv.gz",
        "20230730/AB/ABH4.csv.gz",
        "20230731/AB/ABH4.csv.gz",
        "20230801/AB/ABH4.csv.gz",
        "20230729/AB/ABJ4.csv.gz",
        "20230730/AB/ABJ4.csv.gz",
        "20230731/AB/ABJ4.csv.gz",
        "20230801/AB/ABJ4.csv.gz",
        "20230729/DE/DEH4.csv.gz",
        "20230730/DE/DEH4.csv.gz",
        "20230731/DE/DEH4.csv.gz",
        "20230801/DE/DEH4.csv.gz",
        "20230729/DE/DEJ4.csv.gz",
        "20230730/DE/DEJ4.csv.gz",
        "20230731/DE/DEJ4.csv.gz",
        "20230801/DE/DEJ4.csv.gz",
    }
    actual = set(downloader._generate_object_keys(path_format, filters))
    assert actual == expected


def test_generate_object_keys_futures_data_expdate_with_different_years(tmp_path: Path):
    path_format = "yyyymmdd/ss/ssmy.csv.gz"
    symbols = ["AB", "DE"]
    start_date = "20230729"
    end_date = "20230801"
    expiration_date_start = "20231201"
    expiration_date_end = "20240101"
    filters = downloader.S3KeyFilter(
        symbols=symbols,
        date=(start_date, end_date),
        expiration_date=(expiration_date_start, expiration_date_end),
    )
    expected = {
        "20230729/AB/ABZ3.csv.gz",
        "20230730/AB/ABZ3.csv.gz",
        "20230731/AB/ABZ3.csv.gz",
        "20230801/AB/ABZ3.csv.gz",
        "20230729/AB/ABF4.csv.gz",
        "20230730/AB/ABF4.csv.gz",
        "20230731/AB/ABF4.csv.gz",
        "20230801/AB/ABF4.csv.gz",
        "20230729/DE/DEZ3.csv.gz",
        "20230730/DE/DEZ3.csv.gz",
        "20230731/DE/DEZ3.csv.gz",
        "20230801/DE/DEZ3.csv.gz",
        "20230729/DE/DEF4.csv.gz",
        "20230730/DE/DEF4.csv.gz",
        "20230731/DE/DEF4.csv.gz",
        "20230801/DE/DEF4.csv.gz",
    }
    actual = set(downloader._generate_object_keys(path_format, filters))
    assert actual == expected
