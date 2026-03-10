## Elixir script that creates Iceberg test fixture tables for PyIceberg cross-language validation.
## Run via: mix run test/integration/python_fixtures.exs
##
## Environment:
##   ICEBERG_FIXTURES_DIR — directory to write fixtures (created if absent)

alias Iceberg.Storage.Local
alias Iceberg.Test.DuckDBCompute
alias Iceberg.Table

# ---------------------------------------------------------------------------
# Setup fixtures directory
# ---------------------------------------------------------------------------

fixtures_dir =
  case System.get_env("ICEBERG_FIXTURES_DIR") do
    nil ->
      dir = Path.join(System.tmp_dir!(), "iceberg_fixtures_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      dir

    dir ->
      File.mkdir_p!(dir)
      dir
  end

IO.puts("Writing fixtures to: #{fixtures_dir}")

# Start the local storage agent and point it at the fixtures directory
{:ok, _} = Local.start_link()
Local.set_base_dir(fixtures_dir)

opts = [
  storage: Local,
  compute: DuckDBCompute,
  base_url: fixtures_dir
]

# ---------------------------------------------------------------------------
# Helper: write a parquet file with DuckDB and register it as a snapshot
# ---------------------------------------------------------------------------

defmodule Fixtures.Helpers do
  @moduledoc false

  alias Iceberg.{Metadata, Snapshot}

  @doc """
  Create a parquet data file at `parquet_path` from a DuckDB SELECT `sql`,
  then register it as an append snapshot on `table_name`.
  """
  def write_and_register(table_name, parquet_path, sql, opts) do
    write_sql = "COPY (#{sql}) TO '#{parquet_path}' (FORMAT PARQUET)"
    {:ok, :executed} = DuckDBCompute.execute(:memory, write_sql)

    {:ok, metadata} = Metadata.load(table_name, opts)

    data_pattern =
      "#{opts[:base_url]}/#{table_name}/data/#{Path.basename(parquet_path)}"

    table_schema = List.first(metadata["schemas"])
    sequence_number = (metadata["last-sequence-number"] || 0) + 1
    parent_manifests = current_manifests(metadata)

    snapshot_opts =
      Keyword.merge(opts,
        partition_spec: %{"spec-id" => 0, "fields" => []},
        sequence_number: sequence_number,
        operation: "append",
        table_schema: table_schema,
        parent_manifests: parent_manifests
      )

    {:ok, snapshot} = Snapshot.create(:memory, table_name, data_pattern, snapshot_opts)
    {:ok, updated_metadata} = Metadata.add_snapshot(metadata, snapshot)
    :ok = Metadata.save(table_name, updated_metadata, opts)

    {:ok, snapshot}
  end

  defp current_manifests(metadata) do
    current_id = metadata["current-snapshot-id"]

    if current_id && current_id > 0 do
      metadata["snapshots"]
      |> List.wrap()
      |> Enum.find(&(&1["snapshot-id"] == current_id))
      |> case do
        nil -> []
        snapshot -> snapshot["manifest-entries"] || []
      end
    else
      []
    end
  end
end

# ---------------------------------------------------------------------------
# 1. basic_table
# ---------------------------------------------------------------------------

IO.puts("Creating basic_table...")

basic_table = "basic_table"
basic_data_dir = Path.join([fixtures_dir, basic_table, "data"])
File.mkdir_p!(basic_data_dir)

basic_schema = [
  {:id, "INT", true},
  {:name, "STRING", true}
]

:ok = Table.create(basic_table, basic_schema, opts)

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    basic_table,
    Path.join(basic_data_dir, "data-001.parquet"),
    "SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob' UNION ALL SELECT 3, 'charlie'",
    opts
  )

IO.puts("  basic_table created.")

# ---------------------------------------------------------------------------
# 2. multi_append_table
# ---------------------------------------------------------------------------

IO.puts("Creating multi_append_table...")

multi_table = "multi_append_table"
multi_data_dir = Path.join([fixtures_dir, multi_table, "data"])
File.mkdir_p!(multi_data_dir)

:ok = Table.create(multi_table, basic_schema, opts)

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    multi_table,
    Path.join(multi_data_dir, "data-001.parquet"),
    "SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob'",
    opts
  )

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    multi_table,
    Path.join(multi_data_dir, "data-002.parquet"),
    "SELECT 3 as id, 'charlie' as name UNION ALL SELECT 4, 'dave'",
    opts
  )

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    multi_table,
    Path.join(multi_data_dir, "data-003.parquet"),
    "SELECT 5 as id, 'eve' as name",
    opts
  )

IO.puts("  multi_append_table created.")

# ---------------------------------------------------------------------------
# 3. schema_evolved_table
# ---------------------------------------------------------------------------

IO.puts("Creating schema_evolved_table...")

evolved_table = "schema_evolved_table"
evolved_data_dir = Path.join([fixtures_dir, evolved_table, "data"])
File.mkdir_p!(evolved_data_dir)

evolved_initial_schema = [
  {:id, "INT", true},
  {:name, "STRING", true},
  {:temp, "STRING", false},
  {:count, "INT", false}
]

:ok = Table.create(evolved_table, evolved_initial_schema, opts)

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    evolved_table,
    Path.join(evolved_data_dir, "data-001.parquet"),
    "SELECT 1 as id, 'alice' as name, 'tmp1' as temp, 10 as count UNION ALL SELECT 2, 'bob', 'tmp2', 20",
    opts
  )

# add_column: email string (nullable)
:ok = Table.add_column(evolved_table, %{name: "email", type: "string", required: false}, opts)

# rename_column: name -> full_name
:ok = Table.rename_column(evolved_table, "name", "full_name", opts)

# drop_column: temp
:ok = Table.drop_column(evolved_table, "temp", opts)

# update_column_type: count int -> long
:ok = Table.update_column_type(evolved_table, "count", "long", opts)

IO.puts("  schema_evolved_table created.")

# ---------------------------------------------------------------------------
# 4. expired_table
# ---------------------------------------------------------------------------

IO.puts("Creating expired_table...")

expired_table = "expired_table"
expired_data_dir = Path.join([fixtures_dir, expired_table, "data"])
File.mkdir_p!(expired_data_dir)

:ok = Table.create(expired_table, basic_schema, opts)

{:ok, snap_e1} =
  Fixtures.Helpers.write_and_register(
    expired_table,
    Path.join(expired_data_dir, "data-001.parquet"),
    "SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob'",
    opts
  )

{:ok, snap_e2} =
  Fixtures.Helpers.write_and_register(
    expired_table,
    Path.join(expired_data_dir, "data-002.parquet"),
    "SELECT 3 as id, 'charlie' as name UNION ALL SELECT 4, 'dave'",
    opts
  )

{:ok, _snap_e3} =
  Fixtures.Helpers.write_and_register(
    expired_table,
    Path.join(expired_data_dir, "data-003.parquet"),
    "SELECT 5 as id, 'eve' as name",
    opts
  )

# Expire the first two snapshots explicitly by ID
{:ok, _result} =
  Table.expire_snapshots(expired_table,
    Keyword.merge(opts, snapshot_ids: [snap_e1["snapshot-id"], snap_e2["snapshot-id"]])
  )

IO.puts("  expired_table created.")

# ---------------------------------------------------------------------------
# 5. rewritten_table
# ---------------------------------------------------------------------------

IO.puts("Creating rewritten_table...")

rewritten_table = "rewritten_table"
rewritten_data_dir = Path.join([fixtures_dir, rewritten_table, "data"])
File.mkdir_p!(rewritten_data_dir)

:ok = Table.create(rewritten_table, basic_schema, opts)

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    rewritten_table,
    Path.join(rewritten_data_dir, "data-001.parquet"),
    "SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob'",
    opts
  )

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    rewritten_table,
    Path.join(rewritten_data_dir, "data-002.parquet"),
    "SELECT 3 as id, 'charlie' as name UNION ALL SELECT 4, 'dave'",
    opts
  )

{:ok, _} =
  Fixtures.Helpers.write_and_register(
    rewritten_table,
    Path.join(rewritten_data_dir, "data-003.parquet"),
    "SELECT 5 as id, 'eve' as name",
    opts
  )

{:ok, _} = Table.rewrite_manifests(rewritten_table, opts)

IO.puts("  rewritten_table created.")

# ---------------------------------------------------------------------------
# Write _fixtures.json manifest
# ---------------------------------------------------------------------------

fixtures_manifest = %{
  "fixtures_dir" => fixtures_dir,
  "tables" => %{
    "basic_table" => %{
      "path" => Path.join(fixtures_dir, basic_table),
      "expected_rows" => 3,
      "expected_columns" => ["id", "name"],
      "expected_snapshots" => 1
    },
    "multi_append_table" => %{
      "path" => Path.join(fixtures_dir, multi_table),
      "expected_rows" => 5,
      "expected_columns" => ["id", "name"],
      "expected_snapshots" => 3
    },
    "schema_evolved_table" => %{
      "path" => Path.join(fixtures_dir, evolved_table),
      "expected_columns" => ["id", "full_name", "count", "email"],
      "dropped_columns" => ["temp", "name"]
    },
    "expired_table" => %{
      "path" => Path.join(fixtures_dir, expired_table),
      "expected_rows" => 5,
      "expected_snapshots" => 1
    },
    "rewritten_table" => %{
      "path" => Path.join(fixtures_dir, rewritten_table),
      "expected_rows" => 5
    }
  }
}

manifest_path = Path.join(fixtures_dir, "_fixtures.json")
File.write!(manifest_path, JSON.encode!(fixtures_manifest))

IO.puts("")
IO.puts("Fixtures written to: #{fixtures_dir}")
IO.puts("Manifest: #{manifest_path}")
