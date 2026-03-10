"""Validate snapshot, manifest list, and manifest file structure."""
import fastavro
from conftest import find_latest_metadata, load_metadata


def test_snapshot_exists(fixtures_config):
    """Verify current snapshot exists in metadata."""
    table_info = fixtures_config["tables"]["basic_table"]
    metadata = load_metadata(table_info["path"])

    assert metadata.get("current-snapshot-id") is not None
    assert metadata["current-snapshot-id"] > 0
    assert len(metadata["snapshots"]) >= 1


def test_manifest_list_is_valid_avro(fixtures_config):
    """Verify the manifest list file is readable Avro."""
    table_info = fixtures_config["tables"]["basic_table"]
    metadata = load_metadata(table_info["path"])

    current_id = metadata["current-snapshot-id"]
    current_snap = next(s for s in metadata["snapshots"] if s["snapshot-id"] == current_id)
    manifest_list_path = current_snap["manifest-list"].replace("file://", "")

    with open(manifest_list_path, "rb") as f:
        reader = fastavro.reader(f)
        entries = list(reader)

    assert len(entries) >= 1
    for entry in entries:
        assert "manifest_path" in entry


def test_manifest_is_valid_avro(fixtures_config):
    """Verify manifest files are readable Avro with data file entries."""
    table_info = fixtures_config["tables"]["basic_table"]
    metadata = load_metadata(table_info["path"])

    current_id = metadata["current-snapshot-id"]
    current_snap = next(s for s in metadata["snapshots"] if s["snapshot-id"] == current_id)
    manifest_list_path = current_snap["manifest-list"].replace("file://", "")

    with open(manifest_list_path, "rb") as f:
        manifest_entries = list(fastavro.reader(f))

    for manifest_entry in manifest_entries:
        manifest_path = manifest_entry["manifest_path"].replace("file://", "")
        with open(manifest_path, "rb") as f:
            reader = fastavro.reader(f)
            data_entries = list(reader)

        assert len(data_entries) >= 1
        for entry in data_entries:
            assert "data_file" in entry
            data_file = entry["data_file"]
            assert data_file["file_format"] == "PARQUET"
            assert data_file["record_count"] > 0
            assert data_file["file_size_in_bytes"] > 0


def test_record_counts_match(load_table, fixtures_config):
    """Verify manifest record counts match actual data."""
    table = load_table("basic_table")
    arrow_table = table.scan().to_arrow()
    expected_rows = fixtures_config["tables"]["basic_table"]["expected_rows"]
    assert len(arrow_table) == expected_rows
