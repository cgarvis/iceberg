defmodule Iceberg.Test.DuckDBCompute do
  @moduledoc """
  DuckDB CLI compute backend for integration testing.

  Implements `Iceberg.Compute` behaviour by shelling out to the `duckdb` CLI.
  The connection is the path to the DuckDB database file (or `:memory:` for in-memory).

  ## Usage

      # Use in-memory database
      {:ok, results} = Iceberg.Test.DuckDBCompute.query(":memory:", "SELECT 1 as n")
      # => {:ok, [%{"n" => "1"}]}
  """

  @behaviour Iceberg.Compute

  @impl true
  def query(conn, sql) do
    case run_duckdb(conn, sql) do
      {:ok, output} -> {:ok, parse_csv(output)}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def execute(conn, sql) do
    case run_duckdb(conn, sql) do
      {:ok, _output} -> {:ok, :executed}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def write_data_files(conn, source_query, dest_path, opts) do
    partition_by = opts[:partition_by] || []

    partition_option =
      case partition_by do
        [] -> []
        parts -> ["PARTITION_BY (#{Enum.join(parts, ", ")})"]
      end

    per_thread_option =
      case partition_by do
        [] -> ["PER_THREAD_OUTPUT true"]
        _ -> []
      end

    copy_options =
      ["FORMAT PARQUET"] ++
        partition_option ++
        per_thread_option ++
        ["OVERWRITE_OR_IGNORE true", "FILENAME_PATTERN 'data-{uuid}'"]

    sql = """
    COPY (#{source_query})
    TO '#{dest_path}'
    (#{Enum.join(copy_options, ", ")})
    """

    execute(conn, sql)
  end

  defp run_duckdb(conn, sql) do
    db_path = if conn == :memory, do: ":memory:", else: conn

    # Load iceberg extension before running the query
    full_sql = "INSTALL iceberg; LOAD iceberg; #{sql}"

    case System.cmd("duckdb", [db_path, "-csv", "-c", full_sql], stderr_to_stdout: true) do
      {output, 0} -> {:ok, String.trim(output)}
      {output, _code} -> {:error, output}
    end
  end

  defp parse_csv(""), do: []

  defp parse_csv(output) do
    [header | rows] = String.split(output, "\n")
    columns = parse_csv_row(header)

    Enum.map(rows, fn row ->
      values = parse_csv_row(row)

      columns
      |> Enum.zip(values)
      |> Enum.into(%{})
    end)
  end

  defp parse_csv_row(row) do
    # Simple CSV parsing that handles quoted fields
    row
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.map(fn
      "\"" <> rest -> String.trim_trailing(rest, "\"")
      val -> val
    end)
  end
end
