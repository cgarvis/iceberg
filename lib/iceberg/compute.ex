defmodule Iceberg.Compute do
  @moduledoc """
  Behaviour for compute backends (DuckDB, Spark, Trino, etc.)

  Defines the interface that all compute backends must implement for
  querying Parquet files and executing SQL operations needed by Iceberg.

  ## Example Implementation

      defmodule MyApp.DuckDBCompute do
        @behaviour Iceberg.Compute

        @impl true
        def query(conn, sql) do
          # Execute query and return results
          {:ok, [%{"count" => 100}]}
        end

        @impl true
        def execute(conn, sql) do
          # Execute command
          {:ok, :executed}
        end
      end
  """

  @type connection :: term()
  @type sql :: String.t()
  @type result :: term()
  @type error :: term()

  @doc """
  Executes a SQL query and returns results.

  Used for extracting metadata from Parquet files, gathering statistics, etc.

  ## Parameters
    - connection: Backend-specific connection
    - sql: SQL query to execute

  ## Returns
    - `{:ok, list(map())}` - List of result rows as maps
    - `{:error, reason}` on failure

  ## Example

      query(conn, "SELECT COUNT(*) as count FROM parquet_metadata('s3://bucket/file.parquet')")
      # => {:ok, [%{"count" => 1000}]}
  """
  @callback query(connection, sql) :: {:ok, list(map())} | {:error, error}

  @doc """
  Executes a SQL command (INSERT, CREATE, COPY, etc.)

  Used for writing data files, creating tables, etc.

  ## Parameters
    - connection: Backend-specific connection
    - sql: SQL command to execute

  ## Returns
    - `{:ok, result}` on success
    - `{:error, reason}` on failure

  ## Example

      execute(conn, "COPY (SELECT * FROM staging) TO 's3://bucket/data/'")
      # => {:ok, %{rows_written: 1000}}
  """
  @callback execute(connection, sql) :: {:ok, result} | {:error, error}
end
