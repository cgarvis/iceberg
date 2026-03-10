"""Validate schema evolution operations."""
from pyiceberg.types import LongType


def test_added_column_exists(load_table):
    """Verify added 'email' column is present."""
    table = load_table("schema_evolved_table")
    field_names = [f.name for f in table.schema().fields]
    assert "email" in field_names


def test_renamed_column(load_table):
    """Verify 'name' was renamed to 'full_name'."""
    table = load_table("schema_evolved_table")
    field_names = [f.name for f in table.schema().fields]
    assert "full_name" in field_names
    assert "name" not in field_names


def test_dropped_column(load_table, fixtures_config):
    """Verify dropped columns are absent from schema."""
    table = load_table("schema_evolved_table")
    field_names = [f.name for f in table.schema().fields]
    dropped = fixtures_config["tables"]["schema_evolved_table"]["dropped_columns"]
    for col in dropped:
        assert col not in field_names


def test_promoted_type(load_table):
    """Verify 'count' was promoted from int to long."""
    table = load_table("schema_evolved_table")
    fields = {f.name: f for f in table.schema().fields}
    assert isinstance(fields["count"].field_type, LongType)


def test_final_column_list(load_table, fixtures_config):
    """Verify final schema has exactly the expected columns."""
    table = load_table("schema_evolved_table")
    field_names = [f.name for f in table.schema().fields]
    expected = fixtures_config["tables"]["schema_evolved_table"]["expected_columns"]
    assert field_names == expected
