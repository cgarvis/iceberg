defmodule Iceberg.Integration.DuckDBTest do
  use ExUnit.Case, async: false

  alias Iceberg.Storage.Local
  alias Iceberg.Test.DuckDBCompute

  @moduletag :duckdb

  setup do
    tmpdir =
      Path.join(System.tmp_dir!(), "iceberg_duckdb_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(tmpdir)

    {:ok, _} = Local.start_link()
    Local.set_base_dir(tmpdir)

    on_exit(fn ->
      IO.puts("\n\nTest artifacts left in: #{tmpdir}")
      # File.rm_rf!(tmpdir)
    end)

    %{tmpdir: tmpdir}
  end

  test "DuckDB can read an Iceberg table created by this library", %{tmpdir: tmpdir} do
    table_name = "test_table"
    data_dir = Path.join([tmpdir, table_name, "data"])
    File.mkdir_p!(data_dir)
    parquet_file = Path.join(data_dir, "data-001.parquet")

    # Step 1: Write a Parquet file using DuckDB
    write_sql = """
    COPY (SELECT 1 as id, 'hello' as name UNION ALL SELECT 2, 'world')
    TO '#{parquet_file}' (FORMAT PARQUET)
    """

    {:ok, :executed} = DuckDBCompute.execute(:memory, write_sql)
    assert File.exists?(parquet_file)

    # Step 2: Create the Iceberg table
    schema = [
      {:id, "INT", true},
      {:name, "STRING", true}
    ]

    opts = [
      storage: Local,
      compute: DuckDBCompute,
      base_url: tmpdir
    ]

    :ok = Iceberg.Table.create(table_name, schema, opts)

    # Step 3: Load metadata to get table schema
    {:ok, metadata} = Iceberg.Metadata.load(table_name, opts)
    table_schema = metadata["current-schema"]

    # Step 4: Extract Parquet stats and create a snapshot
    data_pattern = "#{tmpdir}/#{table_name}/data/*.parquet"

    snapshot_opts =
      Keyword.merge(opts,
        partition_spec: %{"spec-id" => 0, "fields" => []},
        sequence_number: 1,
        operation: "append",
        table_schema: table_schema
      )

    {:ok, snapshot} = Iceberg.Snapshot.create(:memory, table_name, data_pattern, snapshot_opts)

    # Step 5: Add snapshot to metadata and save
    {:ok, updated_metadata} = Iceberg.Metadata.add_snapshot(metadata, snapshot)
    :ok = Iceberg.Metadata.save(table_name, updated_metadata, opts)

    # Step 6: Query the table via DuckDB iceberg_scan
    metadata_path = Path.join([tmpdir, table_name, "metadata", "v1.metadata.json"])
    assert File.exists?(metadata_path)

    scan_sql = "SELECT * FROM iceberg_scan('#{metadata_path}') ORDER BY id"
    {:ok, rows} = DuckDBCompute.query(:memory, scan_sql)

    assert length(rows) == 2
    assert Enum.at(rows, 0)["id"] == "1"
    assert Enum.at(rows, 0)["name"] == "hello"
    assert Enum.at(rows, 1)["id"] == "2"
    assert Enum.at(rows, 1)["name"] == "world"
  end
end
