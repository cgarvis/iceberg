defmodule Iceberg.Integration.SchemaEvolutionDuckDBTest do
  use ExUnit.Case, async: false

  alias Iceberg.Storage.Local
  alias Iceberg.Test.DuckDBCompute
  alias Iceberg.Table

  @moduletag :duckdb

  setup do
    tmpdir =
      Path.join(
        System.tmp_dir!(),
        "iceberg_schema_evolution_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmpdir)

    {:ok, _} = Local.start_link()
    Local.set_base_dir(tmpdir)

    on_exit(fn ->
      IO.puts("\n\nTest artifacts left in: #{tmpdir}")
      # File.rm_rf!(tmpdir)
    end)

    %{tmpdir: tmpdir}
  end

  describe "schema evolution with DuckDB compatibility" do
    test "DuckDB can read table after adding a column", %{tmpdir: tmpdir} do
      table_name = "add_column_test_#{System.unique_integer([:positive])}"

      # Step 1: Create table with initial schema and data
      {table_name, opts} = create_table_with_data(table_name, tmpdir)

      # Step 2: Verify DuckDB can read initial table
      metadata_path = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path}') ORDER BY id"
      {:ok, rows} = DuckDBCompute.query(:memory, scan_sql)

      assert length(rows) == 2
      assert Enum.at(rows, 0)["id"] == "1"
      assert Enum.at(rows, 0)["name"] == "alice"

      # Step 3: Add a new column
      :ok = Table.add_column(table_name, %{name: "email", type: "string", required: false}, opts)

      # Step 4: Verify DuckDB can still read the table with new schema
      # The new column should show as NULL for existing rows
      metadata_path_v1 = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path_v1}') ORDER BY id"
      {:ok, rows_after} = DuckDBCompute.query(:memory, scan_sql)

      assert length(rows_after) == 2
      assert Enum.at(rows_after, 0)["id"] == "1"
      assert Enum.at(rows_after, 0)["name"] == "alice"
      # Email should be NULL for existing data (DuckDB returns "NULL" as string)
      assert Enum.at(rows_after, 0)["email"] in [nil, "NULL"]
    end

    test "DuckDB can read table after renaming a column", %{tmpdir: tmpdir} do
      table_name = "rename_column_test_#{System.unique_integer([:positive])}"

      # Step 1: Create table with initial schema and data
      {table_name, opts} = create_table_with_data(table_name, tmpdir)

      # Step 2: Rename column from 'name' to 'full_name'
      :ok = Table.rename_column(table_name, "name", "full_name", opts)

      # Step 3: Verify DuckDB can read the table with renamed column
      metadata_path = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path}') ORDER BY id"
      {:ok, rows} = DuckDBCompute.query(:memory, scan_sql)

      assert length(rows) == 2
      assert Enum.at(rows, 0)["id"] == "1"
      # The column should now be called 'full_name'
      assert Enum.at(rows, 0)["full_name"] == "alice"
      # The old name should not exist
      refute Map.has_key?(Enum.at(rows, 0), "name")
    end

    test "DuckDB can read table after type promotion", %{tmpdir: tmpdir} do
      table_name = "type_promotion_test_#{System.unique_integer([:positive])}"

      # Step 1: Create table with int column
      schema = [
        {:id, "INT", true},
        {:count, "INT", false}
      ]

      opts = create_table_with_int_data(table_name, tmpdir, schema)

      # Step 2: Verify initial data
      metadata_path_v0 = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path_v0}') ORDER BY id"
      {:ok, rows_before} = DuckDBCompute.query(:memory, scan_sql)

      assert length(rows_before) == 2
      assert Enum.at(rows_before, 0)["count"] == "10"

      # Step 3: Promote int to long
      :ok = Table.update_column_type(table_name, "count", "long", opts)

      # Step 4: Verify DuckDB can still read the table
      metadata_path_v1 = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path_v1}') ORDER BY id"
      {:ok, rows_after} = DuckDBCompute.query(:memory, scan_sql)

      assert length(rows_after) == 2
      # Data should still be readable
      assert Enum.at(rows_after, 0)["count"] == "10"
      assert Enum.at(rows_after, 1)["count"] == "20"
    end

    test "DuckDB can read table after dropping a column", %{tmpdir: tmpdir} do
      table_name = "drop_column_test_#{System.unique_integer([:positive])}"

      # Step 1: Create table with extra column
      schema = [
        {:id, "INT", true},
        {:name, "STRING", false},
        {:temp, "STRING", false}
      ]

      opts = create_table_with_extra_column(table_name, tmpdir, schema)

      # Step 2: Drop the temp column
      :ok = Table.drop_column(table_name, "temp", opts)

      # Step 3: Verify DuckDB can read the table without dropped column
      metadata_path = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path}') ORDER BY id"
      {:ok, rows} = DuckDBCompute.query(:memory, scan_sql)

      assert length(rows) == 2
      assert Enum.at(rows, 0)["id"] == "1"
      assert Enum.at(rows, 0)["name"] == "alice"
      # The temp column should not exist
      refute Map.has_key?(Enum.at(rows, 0), "temp")
    end

    test "DuckDB can read table after multiple sequential schema evolutions", %{tmpdir: tmpdir} do
      table_name = "multi_evolution_test_#{System.unique_integer([:positive])}"

      # Step 1: Create initial table
      {table_name, opts} = create_table_with_data(table_name, tmpdir)

      # Verify initial state
      metadata_path_v0 = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
      scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path_v0}') ORDER BY id"
      {:ok, initial_rows} = DuckDBCompute.query(:memory, scan_sql)
      assert length(initial_rows) == 2

      # Step 2: Evolution 1 - Add email column
      :ok = Table.add_column(table_name, %{name: "email", type: "string", required: false}, opts)

      {:ok, rows_v1} = DuckDBCompute.query(:memory, scan_sql)
      assert length(rows_v1) == 2
      assert Map.has_key?(Enum.at(rows_v1, 0), "email")

      # Step 3: Evolution 2 - Rename name to full_name
      :ok = Table.rename_column(table_name, "name", "full_name", opts)

      {:ok, rows_v2} = DuckDBCompute.query(:memory, scan_sql)
      assert length(rows_v2) == 2
      assert Map.has_key?(Enum.at(rows_v2, 0), "full_name")
      refute Map.has_key?(Enum.at(rows_v2, 0), "name")

      # Step 4: Evolution 3 - Add age column
      :ok = Table.add_column(table_name, %{name: "age", type: "int", required: false}, opts)

      {:ok, rows_v3} = DuckDBCompute.query(:memory, scan_sql)
      assert length(rows_v3) == 2
      assert Map.has_key?(Enum.at(rows_v3, 0), "age")

      # Verify final schema has all expected columns
      final_row = Enum.at(rows_v3, 0)
      assert Map.has_key?(final_row, "id")
      assert Map.has_key?(final_row, "full_name")
      assert Map.has_key?(final_row, "email")
      assert Map.has_key?(final_row, "age")
    end

    test "schema version history is preserved and accessible", %{tmpdir: tmpdir} do
      table_name = "version_history_test_#{System.unique_integer([:positive])}"

      # Create table
      {table_name, opts} = create_table_with_data(table_name, tmpdir)

      # Perform multiple evolutions
      :ok = Table.add_column(table_name, %{name: "email", type: "string"}, opts)
      :ok = Table.add_column(table_name, %{name: "age", type: "int"}, opts)
      :ok = Table.rename_column(table_name, "name", "full_name", opts)

      # Verify metadata has all schema versions
      {:ok, metadata} = Iceberg.Metadata.load(table_name, opts)

      # Should have 4 schemas: initial + 3 evolutions
      assert length(metadata["schemas"]) == 4
      assert metadata["current-schema-id"] == 3

      # Verify schema IDs are sequential
      schema_ids = Enum.map(metadata["schemas"], & &1["schema-id"])
      assert schema_ids == [0, 1, 2, 3]

      # Verify field IDs are never reused
      # Each schema should have unique field IDs within itself
      for schema <- metadata["schemas"] do
        field_ids = Enum.map(schema["fields"], & &1["id"])
        assert length(field_ids) == length(Enum.uniq(field_ids))
      end

      # Verify last-column-id tracks the highest ID ever used
      # Should be at least 4 (original 2 fields + 2 added fields)
      assert metadata["last-column-id"] >= 4
    end
  end

  ## Helper Functions

  defp create_table_with_data(table_name, tmpdir) do
    data_dir = Path.join([tmpdir, table_name, "data"])
    File.mkdir_p!(data_dir)
    parquet_file = Path.join(data_dir, "data-001.parquet")

    # Write data using DuckDB
    write_sql = """
    COPY (SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob')
    TO '#{parquet_file}' (FORMAT PARQUET)
    """

    {:ok, :executed} = DuckDBCompute.execute(:memory, write_sql)

    # Create Iceberg table
    schema = [
      {:id, "INT", true},
      {:name, "STRING", false}
    ]

    opts = [
      storage: Local,
      compute: DuckDBCompute,
      base_url: tmpdir
    ]

    :ok = Table.create(table_name, schema, opts)

    # Register the data file with a snapshot
    {:ok, metadata} = Iceberg.Metadata.load(table_name, opts)
    data_pattern = "#{tmpdir}/#{table_name}/data/*.parquet"

    snapshot_opts =
      Keyword.merge(opts,
        partition_spec: %{"spec-id" => 0, "fields" => []},
        sequence_number: 1,
        operation: "append",
        table_schema: metadata["current-schema"]
      )

    {:ok, snapshot} = Iceberg.Snapshot.create(:memory, table_name, data_pattern, snapshot_opts)
    {:ok, updated_metadata} = Iceberg.Metadata.add_snapshot(metadata, snapshot)
    :ok = Iceberg.Metadata.save(table_name, updated_metadata, opts)

    {table_name, opts}
  end

  defp create_table_with_int_data(table_name, tmpdir, schema) do
    data_dir = Path.join([tmpdir, table_name, "data"])
    File.mkdir_p!(data_dir)
    parquet_file = Path.join(data_dir, "data-001.parquet")

    # Write data with int column
    write_sql = """
    COPY (SELECT 1 as id, 10 as count UNION ALL SELECT 2, 20)
    TO '#{parquet_file}' (FORMAT PARQUET)
    """

    {:ok, :executed} = DuckDBCompute.execute(:memory, write_sql)

    opts = [
      storage: Local,
      compute: DuckDBCompute,
      base_url: tmpdir
    ]

    :ok = Table.create(table_name, schema, opts)

    # Register the data
    {:ok, metadata} = Iceberg.Metadata.load(table_name, opts)
    data_pattern = "#{tmpdir}/#{table_name}/data/*.parquet"

    snapshot_opts =
      Keyword.merge(opts,
        partition_spec: %{"spec-id" => 0, "fields" => []},
        sequence_number: 1,
        operation: "append",
        table_schema: metadata["current-schema"]
      )

    {:ok, snapshot} = Iceberg.Snapshot.create(:memory, table_name, data_pattern, snapshot_opts)
    {:ok, updated_metadata} = Iceberg.Metadata.add_snapshot(metadata, snapshot)
    :ok = Iceberg.Metadata.save(table_name, updated_metadata, opts)

    opts
  end

  defp create_table_with_extra_column(table_name, tmpdir, schema) do
    data_dir = Path.join([tmpdir, table_name, "data"])
    File.mkdir_p!(data_dir)
    parquet_file = Path.join(data_dir, "data-001.parquet")

    # Write data with temp column
    write_sql = """
    COPY (SELECT 1 as id, 'alice' as name, 'temp1' as temp UNION ALL SELECT 2, 'bob', 'temp2')
    TO '#{parquet_file}' (FORMAT PARQUET)
    """

    {:ok, :executed} = DuckDBCompute.execute(:memory, write_sql)

    opts = [
      storage: Local,
      compute: DuckDBCompute,
      base_url: tmpdir
    ]

    :ok = Table.create(table_name, schema, opts)

    # Register the data
    {:ok, metadata} = Iceberg.Metadata.load(table_name, opts)
    data_pattern = "#{tmpdir}/#{table_name}/data/*.parquet"

    snapshot_opts =
      Keyword.merge(opts,
        partition_spec: %{"spec-id" => 0, "fields" => []},
        sequence_number: 1,
        operation: "append",
        table_schema: metadata["current-schema"]
      )

    {:ok, snapshot} = Iceberg.Snapshot.create(:memory, table_name, data_pattern, snapshot_opts)
    {:ok, updated_metadata} = Iceberg.Metadata.add_snapshot(metadata, snapshot)
    :ok = Iceberg.Metadata.save(table_name, updated_metadata, opts)

    opts
  end
end
