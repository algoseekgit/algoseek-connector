from algoseek_connector.clickhouse.metadata_api import APIConsumer
from pathlib import Path


def pytest_configure(config):
    # Download table metadata from API
    data_dir_path = Path("tests/data")
    table_data_path = data_dir_path / "table_data.json"
    if not table_data_path.is_file():
        consumer = APIConsumer()
        data_dir_path.mkdir(exist_ok=True)
        consumer.dump_table_metadata(table_data_path)
