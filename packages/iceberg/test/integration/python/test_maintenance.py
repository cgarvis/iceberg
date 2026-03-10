"""Validate maintenance operations (expire_snapshots, rewrite_manifests)."""
from conftest import load_metadata


def test_expired_table_readable(load_table, fixtures_config):
    """Verify table is still readable after expiring snapshots."""
    table = load_table("expired_table")
    arrow_table = table.scan().to_arrow()
    expected = fixtures_config["tables"]["expired_table"]["expected_rows"]
    assert len(arrow_table) == expected


def test_expired_snapshot_count(fixtures_config):
    """Verify only retained snapshots remain in metadata."""
    table_info = fixtures_config["tables"]["expired_table"]
    metadata = load_metadata(table_info["path"])
    expected = fixtures_config["tables"]["expired_table"]["expected_snapshots"]
    assert len(metadata["snapshots"]) == expected


def test_rewritten_table_readable(load_table, fixtures_config):
    """Verify table is still readable after rewriting manifests."""
    table = load_table("rewritten_table")
    arrow_table = table.scan().to_arrow()
    expected = fixtures_config["tables"]["rewritten_table"]["expected_rows"]
    assert len(arrow_table) == expected
