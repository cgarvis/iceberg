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

  @doc """
  Writes data files from a source query to a destination path in Parquet format.

  This is a higher-level operation that abstracts database-specific syntax for
  writing Parquet files with partitioning support.

  ## Parameters
    - connection: Backend-specific connection
    - source_query: SQL SELECT query to use as the data source
    - dest_path: Destination path/URL for writing files (e.g., "s3://bucket/table/data/")
    - opts: Options including:
      - `:partition_by` - List of column names to partition by (default: [])
      - Additional backend-specific options

  ## Returns
    - `{:ok, result}` on success
    - `{:error, reason}` on failure

  ## Example

      write_data_files(conn, "SELECT * FROM staging", "s3://bucket/data/", partition_by: ["date"])
      # => {:ok, :written}
  """
  @callback write_data_files(
              connection,
              source_query :: sql,
              dest_path :: String.t(),
              opts :: keyword()
            ) ::
              {:ok, result} | {:error, error}
end
