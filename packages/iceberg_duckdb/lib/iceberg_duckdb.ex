defmodule IcebergDuckdb do
  @moduledoc """
  DuckDB compute adapter for Apache Iceberg using ADBC.

  Opinionated, production-ready adapter for using Apache Iceberg tables with
  DuckDB via the high-performance ADBC (Arrow Database Connectivity) interface.

  ## Features

  - **Batteries included**: Works out of the box with sensible defaults
  - **S3 support**: Automatic S3 credential configuration from environment
  - **Safe defaults**: Memory limits and thread constraints for production use
  - **Iceberg integration**: Automatic extension loading and configuration

  ## Quick Start

      # Configure in config/runtime.exs
      config :iceberg_duckdb,
        aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
        aws_region: "us-east-1"

      # Open connection (Iceberg support enabled by default)
      {:ok, conn} = IcebergDuckdb.open()

      # Use with Iceberg operations
      opts = [
        storage: MyApp.S3Storage,
        compute: IcebergDuckdb,
        base_url: "s3://my-bucket"
      ]

      {:ok, snapshot} = Iceberg.Table.insert_overwrite(
        conn,
        MyApp.Schema.Events,
        "SELECT * FROM staging",
        opts
      )

      # Close when done
      IcebergDuckdb.close(conn)

  ## Configuration

  Configure via application config:

      # config/runtime.exs
      config :iceberg_duckdb,
        memory_limit: "4GB",
        threads: 4,
        temp_dir: "/tmp/duckdb",
        aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
        aws_region: "us-east-1",
        s3_endpoint: System.get_env("S3_ENDPOINT")  # For MinIO, etc.

  All settings are optional with sensible defaults:
  - `memory_limit` - defaults to "2GB"
  - `threads` - defaults to number of CPU cores
  - `temp_dir` - defaults to system temp directory
  - `aws_region` - defaults to "us-east-1"

  ## Automatic Setup

  When `iceberg: true` is specified, the adapter automatically:
  - Installs and loads the Iceberg extension
  - Installs and loads the httpfs extension (for S3)
  - Configures S3 credentials from application config
  - Sets safe memory and thread limits
  - Enables `force_download` to avoid S3 range request issues

  ## Telemetry

  The adapter emits Telemetry events for observability:
  - `[:iceberg_duckdb, :connection, :open, :start | :stop | :exception]`
  - `[:iceberg_duckdb, :query, :start | :stop | :exception]`
  - `[:iceberg_duckdb, :execute, :start | :stop | :exception]`
  - `[:iceberg_duckdb, :write_data_files, :start | :stop | :exception]`

  Attach handlers to log, monitor, or send metrics to your APM.
  Works seamlessly with Phoenix LiveDashboard.
  """

  @behaviour Iceberg.Compute

  alias IcebergDuckdb.Config

  @doc """
  Opens a DuckDB connection.

  Opens an in-memory database by default, or a persistent database if a path is provided.
  Uses configuration from `IcebergDuckdb.Config` but allows per-connection overrides.

  ## Options

    - `:iceberg` - Load Iceberg extension and configure S3 (default: `true`)
    - `:memory_limit` - Memory limit (e.g., "4GB", overrides config default)
    - `:threads` - Number of threads (overrides config default)
    - `:temp_dir` - Temporary directory path (overrides config default)

  ## Examples

      # In-memory with defaults
      {:ok, conn} = IcebergDuckdb.open()

      # In-memory with custom options
      {:ok, conn} = IcebergDuckdb.open(memory_limit: "8GB")

      # Persistent database
      {:ok, conn} = IcebergDuckdb.open("my_database.db")

      # Persistent with options
      {:ok, conn} = IcebergDuckdb.open("my_database.db", memory_limit: "8GB")

      # Plain DuckDB without Iceberg (rare)
      {:ok, conn} = IcebergDuckdb.open(iceberg: false)
  """
  def open(path \\ nil, opts \\ [])

  def open(nil, opts) when is_list(opts) do
    do_open([driver: :duckdb], opts)
  end

  def open(path, opts) when is_binary(path) and is_list(opts) do
    do_open([driver: :duckdb, path: path], opts)
  end

  defp do_open(db_opts, opts) do
    start_time = System.monotonic_time()

    memory_limit = Keyword.get(opts, :memory_limit, Config.memory_limit())
    threads = Keyword.get(opts, :threads, Config.threads())
    temp_dir = Keyword.get(opts, :temp_dir, Config.temp_dir())
    load_iceberg = Keyword.get(opts, :iceberg, true)

    metadata = %{
      path: Keyword.get(db_opts, :path),
      memory_limit: memory_limit,
      threads: threads,
      iceberg: load_iceberg
    }

    :telemetry.execute(
      [:iceberg_duckdb, :connection, :open, :start],
      %{system_time: System.system_time()},
      metadata
    )

    result =
      with {:ok, db} <- Adbc.Database.start_link(db_opts),
           {:ok, conn} <- Adbc.Connection.start_link(database: db),
           :ok <- configure_duckdb(conn, memory_limit, threads, temp_dir) do
        if load_iceberg do
          with :ok <- setup_extensions(conn),
               :ok <- setup_s3_credentials(conn),
               {:ok, _} <- execute(conn, "SET force_download = true") do
            {:ok, {db, conn}}
          end
        else
          {:ok, {db, conn}}
        end
      end

    duration = System.monotonic_time() - start_time

    case result do
      {:ok, _} ->
        :telemetry.execute(
          [:iceberg_duckdb, :connection, :open, :stop],
          %{duration: duration},
          metadata
        )

      {:error, reason} ->
        :telemetry.execute(
          [:iceberg_duckdb, :connection, :open, :exception],
          %{duration: duration},
          Map.put(metadata, :reason, reason)
        )
    end

    result
  end

  @doc """
  Closes a DuckDB connection and database.

  Accepts either a `{db, conn}` tuple or just a connection.
  """
  def close({db, conn}) do
    try do
      GenServer.stop(conn)
    rescue
      _ -> :ok
    end

    try do
      GenServer.stop(db)
    rescue
      _ -> :ok
    end

    :ok
  end

  def close(conn) do
    GenServer.stop(conn)
    :ok
  rescue
    _ -> :ok
  end

  @doc """
  Runs a function with a temporary DuckDB connection.

  Ensures the connection and database are closed after the function completes.
  Iceberg extension is loaded by default.

  ## Examples

      # Simple usage (Iceberg loaded by default)
      IcebergDuckdb.with_connection(fn conn ->
        Iceberg.Table.create(MySchema, nil, opts)
      end)

      # With custom options
      IcebergDuckdb.with_connection([memory_limit: "8GB"], fn conn ->
        # Large operation
      end)
  """
  def with_connection(opts \\ [], fun) when is_list(opts) and is_function(fun, 1) do
    case open(opts) do
      {:ok, {_db, conn} = handle} ->
        try do
          # Pass just the connection to the function
          fun.(conn)
        after
          close(handle)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Iceberg.Compute callbacks

  @impl true
  def query(conn, sql) do
    start_time = System.monotonic_time()
    metadata = %{sql: sql}

    :telemetry.execute(
      [:iceberg_duckdb, :query, :start],
      %{system_time: System.system_time()},
      metadata
    )

    case Adbc.Connection.query(conn, sql) do
      {:ok, result} ->
        rows = result_to_maps(result)
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:iceberg_duckdb, :query, :stop],
          %{duration: duration, row_count: length(rows)},
          metadata
        )

        {:ok, rows}

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:iceberg_duckdb, :query, :exception],
          %{duration: duration},
          Map.put(metadata, :reason, reason)
        )

        {:error, reason}
    end
  end

  @impl true
  def execute(conn, sql) do
    start_time = System.monotonic_time()
    metadata = %{sql: sql}

    :telemetry.execute(
      [:iceberg_duckdb, :execute, :start],
      %{system_time: System.system_time()},
      metadata
    )

    case Adbc.Connection.query(conn, sql) do
      {:ok, _result} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:iceberg_duckdb, :execute, :stop],
          %{duration: duration},
          metadata
        )

        {:ok, nil}

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:iceberg_duckdb, :execute, :exception],
          %{duration: duration},
          Map.put(metadata, :reason, reason)
        )

        {:error, reason}
    end
  end

  @impl true
  def write_data_files(conn, source_query, dest_path, opts) do
    start_time = System.monotonic_time()
    partition_by = opts[:partition_by] || []

    metadata = %{
      dest_path: dest_path,
      partitioned: !Enum.empty?(partition_by),
      partition_count: length(partition_by)
    }

    :telemetry.execute(
      [:iceberg_duckdb, :write_data_files, :start],
      %{system_time: System.system_time()},
      metadata
    )

    sql = build_copy_statement(source_query, dest_path, opts)

    case execute(conn, sql) do
      {:ok, _} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:iceberg_duckdb, :write_data_files, :stop],
          %{duration: duration},
          metadata
        )

        {:ok, :written}

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:iceberg_duckdb, :write_data_files, :exception],
          %{duration: duration},
          Map.put(metadata, :reason, reason)
        )

        {:error, reason}
    end
  end

  @doc """
  Builds a DuckDB COPY statement for writing Parquet files.

  ## DuckDB-specific optimizations:
  - For unpartitioned tables: Uses `PER_THREAD_OUTPUT` to ensure `FILENAME_PATTERN` is respected
  - For partitioned tables: Uses `PARTITION_BY` (incompatible with `PER_THREAD_OUTPUT`)
  - Always uses `OVERWRITE_OR_IGNORE` since Iceberg clears the directory first
  - Uses `data-{uuid}` pattern for consistent file naming

  ## Parameters
    - source_query: SQL SELECT query
    - dest_path: Destination path (e.g., "s3://bucket/table/data/")
    - opts: Options including `:partition_by` (list of column names)

  ## Returns
    SQL string ready for execution
  """
  def build_copy_statement(source_query, dest_path, opts \\ []) do
    partition_by = opts[:partition_by] || []

    # Build partition option conditionally using pattern matching
    partition_option =
      case partition_by do
        [] -> []
        parts -> ["PARTITION_BY (#{Enum.join(parts, ", ")})"]
      end

    # Use OVERWRITE_OR_IGNORE since we clear the directory before writing
    # Note: DuckDB doesn't allow PER_THREAD_OUTPUT with PARTITION_BY
    # Only use PER_THREAD_OUTPUT for unpartitioned tables to force FILENAME_PATTERN usage
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

    """
    COPY (#{source_query})
    TO '#{dest_path}'
    (#{Enum.join(copy_options, ", ")})
    """
  end

  ## Private functions

  defp configure_duckdb(conn, memory_limit, threads, temp_dir) do
    with {:ok, _} <- execute(conn, "SET memory_limit = '#{memory_limit}'"),
         {:ok, _} <- execute(conn, "SET threads = #{threads}"),
         {:ok, _} <- execute(conn, "SET temp_directory = '#{temp_dir}'"),
         {:ok, _} <- execute(conn, "SET enable_progress_bar = false"),
         {:ok, _} <- execute(conn, "SET preserve_insertion_order = false") do
      :ok
    end
  end

  defp setup_extensions(conn) do
    with {:ok, _} <- execute(conn, "INSTALL httpfs"),
         {:ok, _} <- execute(conn, "LOAD httpfs"),
         {:ok, _} <- execute(conn, "INSTALL iceberg"),
         {:ok, _} <- execute(conn, "LOAD iceberg") do
      :ok
    end
  end

  defp setup_s3_credentials(conn) do
    # Get credentials from Config module
    access_key = Config.aws_access_key_id()
    secret_key = Config.aws_secret_access_key()
    region = Config.aws_region()
    endpoint = Config.s3_endpoint()

    # Only create secret if credentials are provided
    if access_key && secret_key do
      # DuckDB expects just the hostname, not the full URL
      endpoint_host =
        if endpoint do
          endpoint
          |> String.replace(~r"^https?://", "")
          |> String.trim_trailing("/")
        end

      # Build SQL for creating S3 secret
      endpoint_clause = if endpoint_host, do: "ENDPOINT '#{endpoint_host}',", else: ""
      url_style_clause = if endpoint_host, do: "URL_STYLE 'path',", else: ""

      sql = """
      CREATE SECRET (
        TYPE S3,
        KEY_ID '#{access_key}',
        SECRET '#{secret_key}',
        REGION '#{region}',
        #{endpoint_clause}
        #{url_style_clause}
        USE_SSL true
      )
      """

      case execute(conn, sql) do
        {:ok, _} -> :ok
        error -> error
      end
    else
      :ok
    end
  end

  defp result_to_maps(%Adbc.Result{} = result) do
    # Convert Adbc.Result to map of column_name => [values]
    data = Adbc.Result.to_map(result)
    columns = Map.keys(data)

    case columns do
      [] ->
        []

      _ ->
        # Get the number of rows from the first column
        first_col = hd(columns)
        num_rows = length(Map.get(data, first_col, []))

        Enum.map(0..(num_rows - 1), fn row_idx ->
          Enum.reduce(columns, %{}, fn col, acc ->
            value = Enum.at(Map.get(data, col, []), row_idx)
            Map.put(acc, col, value)
          end)
        end)
    end
  end

  defp result_to_maps(_), do: []
end
