from algoseek_connector.clickhouse.metadata_api import APIConsumer, MockAPIConsumer
import pytest


@pytest.fixture(scope="module")
def api_consumer():
    return MockAPIConsumer()


def test_APIConsumer_list_db_groups(api_consumer: APIConsumer):
    data_groups = api_consumer.list_db_groups()
    assert len(data_groups)


def test_APIConsumer_list_db_tables(api_consumer: APIConsumer):
    group = api_consumer.list_db_groups()[0]
    datasets = api_consumer.list_db_tables(group)
    assert len(datasets)


def test_APIConsumer_list_db_tables_invalid_group(api_consumer: APIConsumer):
    group = "InvalidGroupName"
    with pytest.raises(ValueError):
        api_consumer.list_db_tables(group)


def test_APIConsumer_get_db_table_metadata(api_consumer: APIConsumer):
    group_index_with_sql_column = 1
    table_index_with_sql_column = 1
    group = api_consumer.list_db_groups()[group_index_with_sql_column]
    name = api_consumer.list_db_tables(group)[table_index_with_sql_column]
    table_metadata = api_consumer.get_db_table_metadata(group, name)
    assert table_metadata.name == name
    assert table_metadata.group == group


def test_APIConsumer_get_db_table_metadata_invalid_group(api_consumer: APIConsumer):
    group = "InvalidGroupName"
    name = api_consumer.list_db_tables(api_consumer.list_db_groups()[0])[0]
    with pytest.raises(ValueError):
        api_consumer.get_db_table_metadata(group, name)


def test_APIConsumer_get_db_table_metadata_invalid_name(api_consumer: APIConsumer):
    group = api_consumer.list_db_groups()[0]
    name = "InvalidTableName"
    with pytest.raises(ValueError):
        api_consumer.get_db_table_metadata(group, name)
