"""Validate multi-append snapshot accumulation."""
from conftest import load_metadata


def test_total_row_count(load_table, fixtures_config):
    """Verify all appended data is visible."""
    table = load_table("multi_append_table")
    arrow_table = table.scan().to_arrow()
    expected = fixtures_config["tables"]["multi_append_table"]["expected_rows"]
    assert len(arrow_table) == expected


def test_all_data_present(load_table):
    """Verify specific values from each append are present."""
    table = load_table("multi_append_table")
    arrow_table = table.scan().to_arrow()
    ids = sorted(arrow_table.column("id").to_pylist())
    assert ids == [1, 2, 3, 4, 5]


def test_snapshot_count(fixtures_config):
    """Verify expected number of snapshots exist."""
    table_info = fixtures_config["tables"]["multi_append_table"]
    metadata = load_metadata(table_info["path"])
    expected = fixtures_config["tables"]["multi_append_table"]["expected_snapshots"]
    assert len(metadata["snapshots"]) == expected
