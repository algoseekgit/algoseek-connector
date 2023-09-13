from pathlib import Path
from typing import Generator, Optional, Union

import pytest
from pandas import DataFrame
from sqlalchemy import Column, String
from sqlalchemy.sql import Select

from algoseek_connector import base
from algoseek_connector.base import CompiledQuery, date_like


def get_dummy_columns() -> list[base.ColumnDescription]:
    columns = list()
    for k in range(10):
        name = f"Column-{k}"
        t = "ColType"
        description = f"Column {k} description"
        col = base.ColumnDescription(name, t, description)
        columns.append(col)
    return columns


def get_dummy_groups() -> list[str]:
    return [f"Group{k}" for k in range(5)]


def get_dummy_datasets() -> list[str]:
    return [f"DataSet{k}" for k in range(5)]


class MockDescriptorProvider(base.DescriptionProvider):
    def get_datagroup_description(self, group: str) -> base.DataGroupDescription:
        return base.DataGroupDescription(group, f"{group} description.")

    def get_columns_description(self, dataset: str) -> list[base.ColumnDescription]:
        return get_dummy_columns()

    def get_dataset_description(
        self, group: str, dataset: str
    ) -> base.DataSetDescription:
        cols = self.get_columns_description(dataset)
        return base.DataSetDescription(dataset, group, cols)


class MockClient(base.ClientProtocol):
    def compile(self, stmt: Select) -> base.CompiledQuery:
        return base.CompiledQuery("DUMMY QUERY", dict())

    def get_dataset_columns(self, group: str, name: str) -> list[Column]:
        columns = get_dummy_columns()
        return [Column(x.name, String) for x in columns]

    def create_function_handle(self) -> base.FunctionHandle:
        return base.FunctionHandle(["avg", "sum"])

    def execute(
        self, sql: str, parameters: dict | None, output: str, **kwargs
    ) -> Union[dict, DataFrame]:
        return dict()

    def download(
        self,
        dataset: str,
        download_path: Path,
        date: Union[base.date_like, tuple[base.date_like, base.date_like]],
        symbols: Union[str, list[str]],
        expiration_date: Union[date_like, tuple[date_like, date_like], None],
    ):
        pass

    def fetch(self, query: CompiledQuery, **kwargs) -> dict[str, tuple]:
        return dict()

    def fetch_iter(
        self, query: CompiledQuery, size: int, **kwargs
    ) -> Generator[dict[str, tuple], None, None]:
        for k in range(size):
            yield dict()

    def fetch_dataframe(self, query: CompiledQuery, **kwargs) -> DataFrame:
        return DataFrame(data=dict())

    def fetch_iter_dataframe(
        self, query: CompiledQuery, size: int, **kwargs
    ) -> Generator[DataFrame, None, None]:
        for k in range(size):
            yield DataFrame()

    def list_datagroups(self) -> list[str]:
        return get_dummy_groups()

    def list_datasets(self, group: str) -> list[str]:
        return get_dummy_datasets()

    def store_to_s3(
        self,
        query: CompiledQuery,
        bucket: str,
        key: str,
        profile_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        pass


@pytest.fixture(scope="module")
def data_source():
    return base.DataSource(MockClient(), MockDescriptorProvider())


def test_DataSource_list_data_groups(data_source: base.DataSource):
    assert data_source.list_datagroups() == get_dummy_groups()


def test_DataSource_fetch_data_group(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        assert group.description.name == g


def test_DataSource_fetch_data_group_invalid_group_raises_error(
    data_source: base.DataSource,
):
    with pytest.raises(base.InvalidDataGroupName):
        data_source.fetch_datagroup("InvalidDataGroup")


def test_DataGroupMapping_len(data_source: base.DataSource):
    group_mapping = data_source.groups
    assert len(group_mapping) == len(data_source.list_datagroups())


def test_DataGroupMapping_iter(data_source: base.DataSource):
    group_mapping = data_source.groups
    group_names = data_source.list_datagroups()
    for g in group_mapping:
        assert g in group_names


def test_DataGroupFetcher_fetch(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        assert isinstance(group, base.DataGroup)
        assert isinstance(group.description, base.DataGroupDescription)
        assert group.source is data_source


def test_DataGroupFetcher_fetch_multiple_fetch_reference_the_same_instance(
    data_source: base.DataSource,
):
    for g in data_source.list_datagroups():
        group1 = data_source.fetch_datagroup(g)
        group2 = data_source.fetch_datagroup(g)
        assert group1 is group2


def test_DataGroup_shares_description_with_DataGroupFetcher(
    data_source: base.DataSource,
):
    for g in data_source.list_datagroups():
        fetcher = data_source.groups[g]
        group = data_source.fetch_datagroup(g)
        assert group.description is fetcher.description


def test_DataGroup_list_datasets(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        assert group.list_datasets() == get_dummy_datasets()


def test_DataGroup_fetch_dataset(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        for ds in group.list_datasets():
            dataset = group.fetch_dataset(ds)
            assert dataset.group is group
            assert dataset.source is data_source
            assert dataset.description.name == ds


def test_DataGroup_fetch_dataset_invalid_dataset_raises_error(
    data_source: base.DataSource,
):
    with pytest.raises(base.InvalidDataSetName):
        group_name = data_source.list_datagroups()[0]
        group = data_source.fetch_datagroup(group_name)
        group.fetch_dataset("InvalidDatasetName")


def test_DataSetMapping_len(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        assert len(group.datasets) == len(group.list_datasets())


def test_DataSetMapping_iter(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        dataset_names = group.list_datasets()
        for ds in group.datasets:
            assert ds in dataset_names


def test_DataSetFetcher_fetch_multiple_fetch_reference_the_same_instances(
    data_source: base.DataSource,
):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        for ds in group.list_datasets():
            dataset1 = group.fetch_dataset(ds)
            dataset2 = group.fetch_dataset(ds)
            assert dataset1 is dataset2


def test_DataSetFetcher_and_DataSet_shares_description(data_source: base.DataSource):
    for g in data_source.list_datagroups():
        group = data_source.fetch_datagroup(g)
        for ds in group.list_datasets():
            fetcher = group.datasets[ds]
            dataset = group.fetch_dataset(ds)
            assert fetcher.description is dataset.description


@pytest.fixture
def dataset(data_source: base.DataSource):
    group_name = data_source.list_datagroups()[0]
    group = data_source.fetch_datagroup(group_name)
    dataset_name = group.list_datasets()[0]
    return group.fetch_dataset(dataset_name)


def test_DataSet_columns_attributes(dataset: base.DataSet):
    for col_name in get_dummy_columns():
        col = getattr(dataset, col_name.name)
        assert isinstance(col, Column)


def test_DataSet_get_columns_using_keys(dataset: base.DataSet):
    for col_name in get_dummy_columns():
        col = dataset[col_name.name]
        assert isinstance(col, Column)


def test_DataSet_get_columns_using_column_handle_attributes(dataset: base.DataSet):
    column_handle = dataset.get_column_handle()
    for col_name in get_dummy_columns():
        col = getattr(column_handle, col_name.name)
        assert isinstance(col, Column)


def test_DataSet_get_columns_using_column_handle_keys(dataset: base.DataSet):
    column_handle = dataset.get_column_handle()
    for col_name in get_dummy_columns():
        col = column_handle[col_name.name]
        assert isinstance(col, Column)


def test_DataSet_get_function_handle(dataset: base.DataSet):
    function_handle = dataset.get_function_handle()
    assert isinstance(function_handle, base.FunctionHandle)


def test_FunctionHandle_arbitrary_function():
    f = base.FunctionHandle(list())
    f.my_random_function
    assert True


def test_DataSet_select(dataset: base.DataSet):
    stmt = dataset.select()
    assert isinstance(stmt, Select)


def test_DataSet_select_subset_columns(dataset: base.DataSet):
    columns = [x for x in dataset.c]
    columns = columns[: len(columns) // 2]
    stmt = dataset.select(*columns)
    assert isinstance(stmt, Select)


def test_DataSet_select_exclude_columns(dataset: base.DataSet):
    columns = [x for x in dataset.c]
    columns = columns[: len(columns) // 2]
    stmt = dataset.select(exclude=columns)
    assert isinstance(stmt, Select)


def test_DataSet_select_exclude_all_columns_raises_error(dataset: base.DataSet):
    with pytest.raises(ValueError):
        columns = [x for x in dataset.c]
        dataset.select(exclude=columns)


def test_DataSet_execute(dataset: base.DataSet):
    actual = dataset.execute("DUMMY QUERY")
    assert actual == dict()


def test_DataSet_fetch(dataset: base.DataSet):
    stmt = dataset.select()
    actual = dataset.fetch(stmt)
    assert actual == dict()


def test_DataSet_fetch_iter(dataset: base.DataSet):
    stmt = dataset.select()
    size = 10
    for chunk in dataset.fetch_iter(stmt, size):
        assert chunk == dict()


def test_DataSet_fetch_dataframe(dataset: base.DataSet):
    stmt = dataset.select()
    actual = dataset.fetch_dataframe(stmt)
    assert isinstance(actual, DataFrame)
    assert actual.empty


def test_DataSet_fetch_iter_dataframe(dataset: base.DataSet):
    stmt = dataset.select()
    size = 10
    for chunk in dataset.fetch_iter_dataframe(stmt, size):
        assert isinstance(chunk, DataFrame)
        assert chunk.empty


def test_DataSet_compile(dataset: base.DataSet):
    stmt = dataset.select()
    actual = dataset.compile(stmt)
    assert isinstance(actual, base.CompiledQuery)


def test_ColumnDescription_get_type_no_args():
    t = "Type"
    col = base.ColumnDescription("ColumnName", t, "description")
    assert col.get_type_name() == t


def test_ColumnDescription_get_type_with_args():
    t_name = "Type"
    t_args = ["Arg1", "Arg2"]
    t_args_str = ", ".join(t_args)
    t = f"{t_name}({t_args_str})"
    col = base.ColumnDescription("ColumnName", t, "description")
    assert col.get_type_name() == t_name


def test_ColumnDescription_get_types_args_no_args():
    t = "Type"
    col = base.ColumnDescription("ColumnName", t, "description")
    assert col.get_type_args() == list()


def test_ColumnDescription_get_type_args_with_args():
    t_name = "Type"
    t_args = ["Arg1", "Arg2", "Arg3"]
    t_args_str = ", ".join(t_args)
    t = f"{t_name}({t_args_str})"
    col = base.ColumnDescription("ColumnName", t, "description")
    assert col.get_type_args() == t_args


def test_DataSetDescription_no_display_name_uses_dataset_name():
    name = "DatasetName"
    group = "GroupName"
    columns = get_dummy_columns()
    description = base.DataSetDescription(name, group, columns)
    assert description.display_name == name


def test_DataSetDescription_no_description():
    name = "DatasetName"
    group = "GroupName"
    columns = get_dummy_columns()
    description = base.DataSetDescription(name, group, columns)
    assert description.description == ""


def test_DataSetDescription_no_granularity():
    name = "DatasetName"
    group = "GroupName"
    columns = get_dummy_columns()
    description = base.DataSetDescription(name, group, columns)
    assert description.granularity == ""


def test_DataSetDescription_no_pdf_url():
    name = "DatasetName"
    group = "GroupName"
    columns = get_dummy_columns()
    description = base.DataSetDescription(name, group, columns)
    assert description.pdf_url == ""


def test_DataSetDescription_no_sample_data_url():
    name = "DatasetName"
    group = "GroupName"
    columns = get_dummy_columns()
    description = base.DataSetDescription(name, group, columns)
    assert description.sample_data_url == ""


def test_DataSetDescription_get_table_name():
    name = "DatasetName"
    group = "GroupName"
    columns = get_dummy_columns()
    description = base.DataSetDescription(name, group, columns)
    actual = description.get_table_name()
    expected = f"{group}.{name}"
    assert actual == expected


def test_DataGroupDescription_no_display_name():
    group_name = "GroupName"
    description = base.DataGroupDescription(group_name)
    assert description.display_name == group_name


def test_DataGroupDescription_no_description():
    group_name = "GroupName"
    description = base.DataGroupDescription(group_name)
    assert description.description == ""
