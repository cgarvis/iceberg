import json
import os
import pytest
from pyiceberg.table import StaticTable


@pytest.fixture(scope="session")
def fixtures_config():
    fixtures_dir = os.environ.get("ICEBERG_FIXTURES_DIR")
    assert fixtures_dir, "ICEBERG_FIXTURES_DIR env var not set"
    config_path = os.path.join(fixtures_dir, "_fixtures.json")
    with open(config_path) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def fixtures_dir(fixtures_config):
    return fixtures_config["fixtures_dir"]


def find_latest_metadata(table_path):
    """Find the latest v{N}.metadata.json file in the metadata dir."""
    metadata_dir = os.path.join(table_path, "metadata")
    metadata_files = sorted(
        [f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")],
        key=lambda f: int(f.split(".")[0].lstrip("v"))
    )
    return os.path.join(metadata_dir, metadata_files[-1])


def load_metadata(table_path):
    """Load the latest metadata JSON for a table."""
    with open(find_latest_metadata(table_path)) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def load_table(fixtures_config):
    def _load(table_name):
        table_info = fixtures_config["tables"][table_name]
        metadata_path = find_latest_metadata(table_info["path"])
        return StaticTable.from_metadata(f"file://{metadata_path}")
    return _load
