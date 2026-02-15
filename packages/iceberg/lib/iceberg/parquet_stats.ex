defmodule Iceberg.ParquetStats do
  @moduledoc """
  Extracts statistics from Parquet files using a compute backend.

  These statistics are required for creating Iceberg manifests with
  proper file metadata for partition pruning and query optimization.
  """

  alias Iceberg.Error
  require Logger

  @doc """
  Extracts metadata from Parquet files needed for manifest creation.

  ## Parameters
    - conn: Compute backend connection
    - file_pattern: Glob pattern for Parquet files (e.g., "s3://bucket/table/data/*.parquet")
                    Must contain only safe characters: alphanumeric, `/`, `*`, `.`, `-`, `_`, `:`
    - opts: Configuration options (compute backend, etc.)

  ## Returns
    `{:ok, list(map())}` - List of file metadata:
      - `:file_path` - Full path to file
      - `:file_size` - File size in bytes
      - `:record_count` - Number of rows
      - `:partition_values` - Extracted partition values (if pattern contains partitions)

    `{:error, reason}` - Query execution error or invalid file pattern

  ## Security

  The file_pattern is validated to prevent SQL injection. Only trusted patterns
  containing safe characters are allowed.
  """
  @spec extract(term(), String.t(), keyword()) ::
          {:ok, list(map())} | {:error, term()}
  def extract(conn, file_pattern, opts \\ []) do
    with :ok <- validate_file_pattern(file_pattern) do
      execute_stats_query(conn, file_pattern, opts)
    end
  end

  # Validates file pattern contains only safe characters to prevent SQL injection
  # Allowed: alphanumeric, /, *, ., -, _, :
  defp validate_file_pattern(pattern) when is_binary(pattern) do
    if String.match?(pattern, ~r/^[a-zA-Z0-9\/\*\.\-_:]+$/) do
      :ok
    else
      Error.invalid_file_pattern("Pattern contains unsafe characters: #{pattern}")
    end
  end

  defp validate_file_pattern(_), do: Error.invalid_file_pattern("Pattern must be a string")

  defp execute_stats_query(conn, file_pattern, opts) do
    compute = Iceberg.Config.compute_backend(opts)

    # parquet_metadata returns one row per column per row group
    # First deduplicate to row-group level, then aggregate to file level
    sql = """
    WITH row_groups AS (
      SELECT DISTINCT
        file_name,
        row_group_id,
        row_group_num_rows,
        row_group_bytes
      FROM parquet_metadata('#{file_pattern}')
    )
    SELECT
      file_name as file_path,
      SUM(row_group_bytes) as file_size_in_bytes,
      SUM(row_group_num_rows) as record_count
    FROM row_groups
    GROUP BY file_name
    ORDER BY file_name
    """

    case compute.query(conn, sql) do
      {:ok, results} ->
        # Convert results to manifest-friendly format
        stats =
          Enum.map(results, fn row ->
            %{
              file_path: row["file_path"],
              file_size_in_bytes: to_integer(row["file_size_in_bytes"]),
              record_count: to_integer(row["record_count"]),
              partition_values: extract_partition_from_path(row["file_path"])
            }
          end)

        {:ok, stats}

      {:error, %{message: message}} when is_binary(message) ->
        # Handle "No files found" error - return empty stats
        if String.contains?(message, "No files found") do
          Logger.debug(fn -> "No parquet files found for pattern: #{file_pattern}" end)
          {:ok, []}
        else
          {:error, message}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Extracts detailed column statistics from Parquet files.

  This is more expensive than `extract/2` but provides column-level stats
  for better query optimization.

  ## Parameters
    - conn: Compute backend connection
    - file_path: Path to specific Parquet file

  ## Returns
    `{:ok, map()}` - Column statistics:
      - `:column_sizes` - Map of column_id => size in bytes
      - `:value_counts` - Map of column_id => value count
      - `:null_value_counts` - Map of column_id => null count
      - `:lower_bounds` - Map of column_id => min value (as bytes)
      - `:upper_bounds` - Map of column_id => max value (as bytes)
  """
  @spec extract_column_stats(term(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def extract_column_stats(_conn, _file_path) do
    # For now, return empty stats - this can be enhanced later
    # with column-level statistics extraction
    {:ok,
     %{
       column_sizes: nil,
       value_counts: nil,
       null_value_counts: nil,
       lower_bounds: nil,
       upper_bounds: nil
     }}
  end

  ## Private Functions

  # Converts numeric types to integer
  defp to_integer(i) when is_integer(i), do: i
  defp to_integer(f) when is_float(f), do: round(f)
  defp to_integer(s) when is_binary(s), do: String.to_integer(s)
  defp to_integer(nil), do: 0
  # Handle Decimal structs (check if it's a struct with __struct__: Decimal)
  defp to_integer(d) when is_map(d) do
    case Map.get(d, :__struct__) do
      mod when is_atom(mod) ->
        # Use runtime check for Decimal module to avoid compile-time warning
        if mod |> Atom.to_string() |> String.ends_with?("Decimal") do
          # Call Decimal.to_integer/1 dynamically to avoid compile-time dependency
          apply(mod, :to_integer, [d])
        else
          raise ArgumentError, "Cannot convert #{inspect(d)} to integer"
        end

      _ ->
        raise ArgumentError, "Cannot convert #{inspect(d)} to integer"
    end
  end

  # Extracts partition values from Hive-style partitioned paths
  # e.g., "s3://bucket/table/data/date=2024-01-15/file.parquet" -> %{"date" => "2024-01-15"}
  defp extract_partition_from_path(file_path) do
    for segment <- String.split(file_path, "/"),
        String.contains?(segment, "="),
        [key, value] <- [String.split(segment, "=", parts: 2)],
        into: %{} do
      {key, value}
    end
  end
end
