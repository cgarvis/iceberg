"""Validate basic table creation and metadata structure."""
from conftest import load_metadata
from pyiceberg.types import IntegerType, StringType


def test_metadata_format_version(fixtures_config):
    """Verify metadata JSON has format-version 2."""
    table_info = fixtures_config["tables"]["basic_table"]
    metadata = load_metadata(table_info["path"])
    assert metadata["format-version"] == 2


def test_schema_fields(load_table, fixtures_config):
    """Verify schema field names and types match expected."""
    table = load_table("basic_table")
    schema = table.schema()
    field_names = [f.name for f in schema.fields]
    expected = fixtures_config["tables"]["basic_table"]["expected_columns"]
    assert field_names == expected


def test_schema_field_types(load_table):
    """Verify schema field types."""
    table = load_table("basic_table")
    schema = table.schema()
    fields = {f.name: f for f in schema.fields}
    assert isinstance(fields["id"].field_type, IntegerType)
    assert isinstance(fields["name"].field_type, StringType)


def test_scan_returns_expected_rows(load_table, fixtures_config):
    """Verify scanning returns expected row count."""
    table = load_table("basic_table")
    arrow_table = table.scan().to_arrow()
    expected = fixtures_config["tables"]["basic_table"]["expected_rows"]
    assert len(arrow_table) == expected


def test_data_values(load_table):
    """Verify actual data values are correct."""
    table = load_table("basic_table")
    arrow_table = table.scan().to_arrow()
    ids = sorted(arrow_table.column("id").to_pylist())
    names = sorted(arrow_table.column("name").to_pylist())
    assert ids == [1, 2, 3]
    assert names == ["alice", "bob", "charlie"]
