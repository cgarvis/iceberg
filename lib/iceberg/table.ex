defmodule Iceberg.Table do
  @moduledoc """
  Public API for Iceberg table operations.

  Supports both Schema modules (Ecto-like DSL) and legacy tuple-based schemas.

  ## Examples

      # With Schema module:
      Iceberg.Table.create(MyApp.Schemas.Events, nil, opts)

      # With legacy tuple-based schema:
      schema = [
        {:id, "STRING", true},
        {:timestamp, "TIMESTAMP", true}
      ]
      Iceberg.Table.create("canonical/events", schema,
        partition_spec: %{"spec-id" => 0, "fields" => []},
        storage: MyApp.Storage,
        compute: MyApp.Compute,
        base_url: "s3://bucket")

  ## Concurrency Warning

  This library does **not** provide internal locking for concurrent writes to the same table.

  If multiple processes may write to the same table simultaneously, you must coordinate access
  externally using one of these approaches:

  - Database locks
  - Distributed locks (e.g., using Redis, etcd, or Consul)
  - Serialize writes through a GenServer
  - Application-level coordination

  **Safe concurrent operations:**
  - Multiple concurrent reads (always safe)
  - Concurrent writes to different tables (safe)

  **Unsafe concurrent operations:**
  - Multiple processes writing to the same table (race conditions on metadata updates)

  Without coordination, concurrent writes may result in:
  - Lost snapshots (one write overwrites another's metadata)
  - Corrupted version-hint.text
  - Inconsistent metadata state
  """

  alias Iceberg.{Error, Metadata, Snapshot}
  require Logger

  @doc """
  Creates a new Iceberg table.

  ## Parameters
    - schema_module_or_path: Either a Schema module or table path string
    - schema: (Optional) Tuple-based schema for legacy API
    - opts: Options
      - `:partition_spec` - Partition specification (for legacy API)
      - `:properties` - Table properties

  ## Returns
    `:ok` - Table created successfully
    `{:error, :already_exists}` - Table already exists
    `{:error, reason}` - Creation failed
  """
  @spec create(module() | String.t(), list(tuple()) | nil, keyword()) ::
          :ok | {:error, term()}
  def create(schema_module_or_path, schema \\ nil, opts \\ [])

  # Schema module API
  def create(schema_module, nil, opts) when is_atom(schema_module) do
    table_path = schema_module.__table_path__()
    iceberg_schema = schema_module.__schema__()
    partition_spec = schema_module.__partition_spec__()

    create_table_impl(table_path, iceberg_schema, partition_spec, opts)
  end

  # Legacy tuple-based API
  def create(table_path, schema, opts) when is_binary(table_path) and is_list(schema) do
    iceberg_schema = convert_legacy_schema(schema)
    partition_spec = Keyword.get(opts, :partition_spec, %{"spec-id" => 0, "fields" => []})

    create_table_impl(table_path, iceberg_schema, partition_spec, opts)
  end

  @spec create_table_impl(String.t(), map(), map(), keyword()) :: :ok | {:error, term()}
  defp create_table_impl(table_path, iceberg_schema, partition_spec, opts) do
    if Metadata.exists?(table_path, opts) do
      Logger.debug(fn -> "Table already exists: #{table_path}" end)
      Error.table_exists()
    else
      Logger.info(fn -> "Creating Iceberg table: #{table_path}" end)

      with {:ok, metadata} <-
             Metadata.create_initial(table_path, iceberg_schema, partition_spec, opts),
           :ok <- Metadata.save(table_path, metadata, opts) do
        Logger.info(fn -> "Created table: #{table_path}" end)
        :ok
      else
        {:error, reason} = error ->
          Logger.error(fn -> "Failed to create table #{table_path}: #{inspect(reason)}" end)
          error
      end
    end
  end

  @doc """
  Inserts data, replacing all existing data (overwrite mode).

  ## Parameters
    - conn: Compute backend connection
    - schema_module_or_path: Schema module or table path string
    - source_query: SQL query returning data to insert
    - opts: Options
      - `:partition_by` - List of partition column names (for legacy API)
      - `:source_file` - Source file identifier for lineage
      - `:operation` - Operation type (default: "overwrite")

  ## Returns
    `{:ok, snapshot}` - Snapshot metadata
    `{:error, reason}` - Insert failed
  """
  @spec insert_overwrite(term(), module() | String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def insert_overwrite(conn, schema_module_or_path, source_query, opts \\ [])

  # Schema module API
  def insert_overwrite(conn, schema_module, source_query, opts) when is_atom(schema_module) do
    table_path = schema_module.__table_path__()
    partition_spec = schema_module.__partition_spec__()
    partition_field = schema_module.__partition_field__()

    partition_by =
      if partition_field do
        [partition_field]
      else
        opts[:partition_by] || []
      end

    insert_overwrite_impl(
      conn,
      table_path,
      source_query,
      partition_spec,
      Keyword.put(opts, :partition_by, partition_by)
    )
  end

  # Legacy API
  def insert_overwrite(conn, table_path, source_query, opts) when is_binary(table_path) do
    case Metadata.load(table_path, opts) do
      {:ok, metadata} ->
        partition_spec =
          List.first(metadata["partition-specs"]) || %{"spec-id" => 0, "fields" => []}

        partition_by = opts[:partition_by] || []

        insert_overwrite_impl(
          conn,
          table_path,
          source_query,
          partition_spec,
          Keyword.put(opts, :partition_by, partition_by)
        )

      {:error, reason} ->
        {:error, {:metadata_load_failed, reason}}
    end
  end

  @spec insert_overwrite_impl(term(), String.t(), String.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defp insert_overwrite_impl(conn, table_path, source_query, partition_spec, opts) do
    Logger.info(fn -> "Starting insert overwrite for table: #{table_path}" end)

    with {:ok, metadata} <- Metadata.load(table_path, opts),
         :ok <- clear_data_directory(table_path, opts),
         :ok <- write_data_files(conn, table_path, source_query, opts) do
      # Compute derived values after successful operations
      data_pattern = Iceberg.Config.full_url("#{table_path}/data/**/*.parquet", opts)
      sequence_number = (metadata["last-sequence-number"] || 0) + 1

      snapshot_opts =
        Keyword.merge(opts,
          partition_spec: partition_spec,
          sequence_number: sequence_number,
          operation: opts[:operation] || "overwrite"
        )

      # Create snapshot and update metadata
      with {:ok, snapshot} <- Snapshot.create(conn, table_path, data_pattern, snapshot_opts),
           {:ok, new_metadata} <- Metadata.add_snapshot(metadata, snapshot),
           :ok <- Metadata.save(table_path, new_metadata, opts) do
        Logger.info(fn ->
          "Insert overwrite complete for #{table_path}: snapshot #{snapshot["snapshot-id"]}"
        end)

        {:ok, snapshot}
      else
        {:error, reason} = error ->
          Logger.error(fn -> "Snapshot creation failed for #{table_path}: #{inspect(reason)}" end)
          error
      end
    else
      {:error, reason} = error ->
        Logger.error(fn -> "Insert overwrite failed for #{table_path}: #{inspect(reason)}" end)
        error
    end
  end

  @doc """
  Checks if an Iceberg table exists and is readable.
  """
  @spec exists?(module() | String.t(), keyword()) :: boolean()
  def exists?(schema_module, opts \\ [])

  def exists?(schema_module, opts) when is_atom(schema_module) do
    table_path = schema_module.__table_path__()
    Metadata.exists?(table_path, opts)
  end

  def exists?(table_path, opts) when is_binary(table_path) do
    Metadata.exists?(table_path, opts)
  end

  @doc """
  Ensures a table has the schema.name-mapping.default property.

  This is needed for tables created before the name mapping feature was added.
  The property maps column names to field IDs so compute engines can read
  Parquet files that don't have embedded field IDs.

  ## Parameters
    - schema_module_or_path: Schema module or table path string
    - opts: Configuration options

  ## Returns
    `:ok` - Name mapping ensured (added or already present)
    `{:error, reason}` - Failed
  """
  @spec ensure_name_mapping(module() | String.t(), keyword()) :: :ok | {:error, term()}
  def ensure_name_mapping(schema_module, opts \\ [])

  def ensure_name_mapping(schema_module, opts) when is_atom(schema_module) do
    table_path = schema_module.__table_path__()
    iceberg_schema = schema_module.__schema__()
    ensure_name_mapping_impl(table_path, iceberg_schema, opts)
  end

  def ensure_name_mapping(table_path, opts) when is_binary(table_path) do
    case Metadata.load(table_path, opts) do
      {:ok, metadata} ->
        schema = List.first(metadata["schemas"]) || %{"fields" => []}
        ensure_name_mapping_impl(table_path, schema, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec ensure_name_mapping_impl(String.t(), map(), keyword()) :: :ok | {:error, term()}
  defp ensure_name_mapping_impl(table_path, schema, opts) do
    case Metadata.load(table_path, opts) do
      {:ok, metadata} ->
        properties = metadata["properties"] || %{}

        if Map.has_key?(properties, "schema.name-mapping.default") do
          Logger.debug(fn -> "Name mapping already exists for #{table_path}" end)
          :ok
        else
          name_mapping = build_name_mapping_json(schema)
          Logger.info(fn -> "Adding name mapping to #{table_path}" end)

          Metadata.update_properties(
            table_path,
            %{"schema.name-mapping.default" => name_mapping},
            opts
          )
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec build_name_mapping_json(map()) :: String.t()
  defp build_name_mapping_json(%{"fields" => fields}) when is_list(fields) do
    mapping =
      Enum.map(fields, fn field ->
        %{
          "field-id" => field["id"],
          "names" => [field["name"]]
        }
      end)

    JSON.encode!(mapping)
  end

  defp build_name_mapping_json(_schema), do: "[]"

  @doc """
  Registers externally-written Parquet files into an Iceberg table.

  Used when files are written outside this library.
  Creates a new snapshot with the provided files.

  ## Parameters
    - conn: Compute backend connection
    - table_path: Relative path to the table
    - file_pattern: Glob pattern matching files (e.g., "table/data/**/prefix-*.parquet")
    - opts: Options including:
      - `:source_file` - Source file identifier for lineage
      - `:operation` - Operation type (default: "append")

  ## Returns
    `{:ok, snapshot}` on success
    `{:error, reason}` on failure
  """
  @spec register_files(term(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def register_files(conn, table_path, file_pattern, opts \\ []) do
    storage = Iceberg.Config.storage_backend(opts)

    Logger.info(fn ->
      "Registering files for table: #{table_path} with pattern: #{file_pattern}"
    end)

    matching_files = find_matching_files(storage, table_path, file_pattern)

    if matching_files == [] do
      Logger.debug(fn -> "No files matched pattern #{file_pattern}" end)
      {:ok, nil}
    else
      register_matching_files(conn, table_path, file_pattern, matching_files, opts)
    end
  end

  @spec find_matching_files(module(), String.t(), String.t()) :: list(String.t())
  defp find_matching_files(storage, table_path, file_pattern) do
    # Extract the prefix from the pattern (before any wildcard)
    file_prefix =
      file_pattern
      |> String.replace("#{table_path}/data/", "")
      |> String.split("*")
      |> List.first()
      |> String.trim_trailing("/")

    # List files matching the pattern
    data_prefix = "#{table_path}/data/"
    files = storage.list(data_prefix)

    Enum.filter(files, fn f ->
      file_prefix == "" || String.contains?(f, file_prefix)
    end)
  end

  @spec register_matching_files(term(), String.t(), String.t(), list(String.t()), keyword()) ::
          {:ok, map()} | {:error, term()}
  defp register_matching_files(conn, table_path, file_pattern, matching_files, opts) do
    with {:ok, metadata} <- Metadata.load(table_path, opts),
         snapshot_opts <- build_snapshot_opts(metadata, file_pattern, opts),
         {:ok, snapshot} <-
           Snapshot.create(conn, table_path, snapshot_opts[:data_url], snapshot_opts),
         {:ok, new_metadata} <- Metadata.add_snapshot(metadata, snapshot),
         :ok <- Metadata.save(table_path, new_metadata, opts) do
      Logger.info(fn ->
        "Registered #{length(matching_files)} files in #{table_path}: snapshot #{snapshot["snapshot-id"]}"
      end)

      {:ok, snapshot}
    end
  end

  @spec build_snapshot_opts(map(), String.t(), keyword()) :: keyword()
  defp build_snapshot_opts(metadata, file_pattern, opts) do
    partition_spec = List.first(metadata["partition-specs"]) || %{"spec-id" => 0, "fields" => []}
    sequence_number = (metadata["last-sequence-number"] || 0) + 1
    operation = opts[:operation] || "append"
    source_file = opts[:source_file]
    table_schema = List.first(metadata["schemas"])
    schema_id = metadata["current-schema-id"] || 0
    data_url_pattern = Iceberg.Config.full_url(file_pattern, opts)

    Keyword.merge(opts,
      partition_spec: partition_spec,
      sequence_number: sequence_number,
      operation: operation,
      table_schema: table_schema,
      schema_id: schema_id,
      source_file: source_file,
      data_url: data_url_pattern
    )
  end

  ## Private Functions

  @spec write_data_files(term(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  defp write_data_files(conn, table_path, source_query, opts) do
    compute = Iceberg.Config.compute_backend(opts)
    partition_by = opts[:partition_by] || []

    data_path = Iceberg.Config.full_url("#{table_path}/data/", opts)

    # Build partition option conditionally using pattern matching
    partition_option =
      case partition_by do
        [] -> []
        parts -> ["PARTITION_BY (#{Enum.join(parts, ", ")})"]
      end

    # Build all COPY options - use FILENAME_PATTERN with relative path
    copy_options = ["FORMAT PARQUET"] ++ partition_option ++ ["FILENAME_PATTERN 'data-{uuid}'"]

    sql = """
    COPY (#{source_query})
    TO '#{data_path}'
    (#{Enum.join(copy_options, ", ")})
    """

    Logger.debug(fn -> "Writing data files with COPY: #{data_path}" end)
    Logger.debug(fn -> "SQL: #{sql}" end)

    case compute.execute(conn, sql) do
      {:ok, _} ->
        Logger.info(fn -> "Data files written successfully" end)
        :ok

      {:error, reason} ->
        Logger.error(fn -> "Failed to write data files: #{inspect(reason)}" end)
        Error.copy_failed(reason)
    end
  end

  @spec clear_data_directory(String.t(), keyword()) :: :ok
  defp clear_data_directory(table_path, opts) do
    storage = Iceberg.Config.storage_backend(opts)
    data_prefix = "#{table_path}/data/"

    case storage.list(data_prefix) do
      files when is_list(files) ->
        delete_files_with_error_tracking(files, storage, data_prefix)

      {:error, _reason} ->
        # If directory doesn't exist, that's fine
        Logger.debug(fn -> "No existing data directory to clear" end)
        :ok
    end
  end

  @spec delete_files_with_error_tracking(list(String.t()), module(), String.t()) :: :ok
  defp delete_files_with_error_tracking(files, storage, data_prefix) do
    # Delete all data files and collect results
    results = Enum.map(files, &safe_delete(storage, &1))
    errors = Enum.filter(results, &match?({:error, _}, &1))

    if Enum.empty?(errors) do
      Logger.debug(fn -> "Cleared #{length(files)} files from #{data_prefix}" end)
      :ok
    else
      Logger.warning(fn ->
        "Failed to delete #{length(errors)} of #{length(files)} files from #{data_prefix}"
      end)

      # Still return :ok as partial cleanup is acceptable for overwrite operations
      # If this is a problem, change to {:error, {:partial_cleanup, errors}}
      :ok
    end
  end

  @spec safe_delete(module(), String.t()) :: :ok | {:error, term()}
  defp safe_delete(storage, file) do
    storage.delete(file)
  end

  # Converts legacy tuple-based schema to Iceberg schema format
  @spec convert_legacy_schema(list(tuple())) :: map()
  defp convert_legacy_schema(schema) do
    fields =
      Enum.with_index(schema, fn {name, type, required}, idx ->
        %{
          "id" => idx + 1,
          "name" => to_string(name),
          "required" => required,
          "type" => normalize_type(type)
        }
      end)

    %{
      "type" => "struct",
      "schema-id" => 0,
      "fields" => fields
    }
  end

  @spec normalize_type(String.t() | atom()) :: String.t()
  defp normalize_type(type) when is_binary(type), do: String.downcase(type)
  defp normalize_type(type) when is_atom(type), do: to_string(type) |> String.downcase()
end
