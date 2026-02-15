defmodule Iceberg.Metadata do
  @moduledoc """
  Manages Iceberg table metadata (v{N}.metadata.json files).

  Handles loading, updating, and saving table metadata including:
  - Schema definitions
  - Partition specifications
  - Sort orders
  - Snapshots
  - Snapshot history
  """

  alias Iceberg.UUID
  require Logger

  @doc """
  Loads the current metadata for a table.

  Reads version-hint.text to find latest version, then loads that metadata file.

  ## Parameters
    - table_path: Relative path to table (e.g., "canonical/port_call_events")
    - opts: Configuration options (storage, base_url, etc.)

  ## Returns
    `{:ok, metadata}` - Loaded metadata map
    `{:error, :not_found}` - Table doesn't exist
    `{:error, reason}` - Other errors
  """
  @spec load(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def load(table_path, opts \\ []) do
    storage = Iceberg.Config.storage_backend(opts)

    with {:ok, version} <- read_version_hint(table_path, opts),
         {:ok, content} <- storage.download("#{table_path}/metadata/v#{version}.metadata.json") do
      {:ok, JSON.decode!(content)}
    else
      {:error, :not_found} = error ->
        Logger.debug(fn -> "Metadata not found for table: #{table_path}" end)
        error

      {:error, reason} = error ->
        Logger.error(fn -> "Failed to load metadata for #{table_path}: #{inspect(reason)}" end)
        error
    end
  end

  @doc """
  Creates initial metadata for a new table.

  ## Parameters
    - table_path: Relative path to table
    - schema: Iceberg schema definition
    - partition_spec: Partition specification
    - opts: Additional options
      - `:location` - Table location (defaults to lake bucket + table_path)
      - `:properties` - Table properties map

  ## Returns
    `{:ok, metadata}` - Initial metadata map
  """
  @spec create_initial(String.t(), map(), map(), keyword()) :: {:ok, map()}
  def create_initial(table_path, schema, partition_spec, opts \\ []) do
    location = Keyword.get(opts, :location, Iceberg.Config.full_url(table_path, opts))
    user_properties = Keyword.get(opts, :properties, %{})

    # Generate name mapping for compute backend compatibility
    # Some engines require field-id mapping when Parquet files
    # don't have embedded field IDs
    name_mapping = build_name_mapping(schema)

    properties =
      Map.merge(
        %{"schema.name-mapping.default" => name_mapping},
        user_properties
      )

    metadata = %{
      "format-version" => 2,
      "table-uuid" => UUID.generate(),
      "location" => location,
      "last-sequence-number" => 0,
      "last-updated-ms" => System.system_time(:millisecond),
      "last-column-id" => count_fields(schema),
      "schemas" => [schema],
      "current-schema-id" => 0,
      "partition-specs" => [partition_spec],
      "default-spec-id" => 0,
      "last-partition-id" => count_partition_fields(partition_spec),
      "properties" => properties,
      "current-snapshot-id" => -1,
      "snapshots" => [],
      "snapshot-log" => [],
      "metadata-log" => [],
      "sort-orders" => [
        %{
          "order-id" => 0,
          "fields" => []
        }
      ],
      "default-sort-order-id" => 0
    }

    {:ok, metadata}
  end

  @doc """
  Updates table properties in metadata.

  ## Parameters
    - table_path: Relative path to table
    - properties: Map of properties to merge
    - opts: Configuration options

  ## Returns
    `:ok` - Properties updated
    `{:error, reason}` - Update failed
  """
  @spec update_properties(String.t(), map(), keyword()) :: :ok | {:error, term()}
  def update_properties(table_path, properties, opts \\ []) do
    with {:ok, metadata} <- load(table_path, opts) do
      current_properties = metadata["properties"] || %{}
      new_properties = Map.merge(current_properties, properties)

      updated_metadata =
        metadata
        |> Map.put("properties", new_properties)
        |> Map.put("last-updated-ms", System.system_time(:millisecond))

      save(table_path, updated_metadata, opts)
    end
  end

  @doc """
  Adds a snapshot to metadata and increments sequence number.

  ## Parameters
    - metadata: Current metadata map
    - snapshot: Snapshot to add

  ## Returns
    `{:ok, updated_metadata}` - Metadata with new snapshot
  """
  @spec add_snapshot(map(), map()) :: {:ok, map()}
  def add_snapshot(metadata, snapshot) do
    sequence_number = (metadata["last-sequence-number"] || 0) + 1
    snapshot_id = snapshot["snapshot-id"]

    updated_metadata =
      metadata
      |> Map.put("current-snapshot-id", snapshot_id)
      |> Map.put("last-sequence-number", sequence_number)
      |> Map.put("last-updated-ms", System.system_time(:millisecond))
      |> Map.update("snapshots", [snapshot], &(&1 ++ [snapshot]))
      |> Map.update("snapshot-log", [], fn log ->
        log ++
          [
            %{
              "snapshot-id" => snapshot_id,
              "timestamp-ms" => snapshot["timestamp-ms"]
            }
          ]
      end)

    {:ok, updated_metadata}
  end

  @doc """
  Evolves a table's schema by applying a schema evolution operation.

  Creates a new schema version and updates metadata:
  - Adds new schema to schemas array
  - Updates current-schema-id
  - Updates last-column-id
  - Updates name mapping
  - Preserves old schemas for historical field ID tracking

  ## Parameters
    - table_path: Relative path to table
    - evolution_fn: Function that takes (current_schema, context) and returns evolved schema
      Context contains:
        - `:next_field_id` - Next field ID to use (from metadata's last-column-id + 1)
        - `:historical_schemas` - Previous schemas for field ID tracking
      Example: `fn schema, ctx -> SchemaEvolution.add_column(schema, %{name: "new_field", type: "string"}, ctx) end`
    - opts: Configuration options
      - `:mode` - Validation mode (:strict, :permissive, :none)
      - `:force` - Skip validation (same as mode: :none)
      - `:table_empty?` - Whether table has data
      - `:storage`, `:base_url` - Storage configuration

  ## Returns
    `{:ok, updated_metadata}` - Metadata with evolved schema
    `{:ok, updated_metadata, warnings}` - Success with warnings (permissive mode)
    `{:error, reason}` - Evolution or validation failed

  ## Examples

      # Add a column
      evolve_schema(table_path, fn schema, ctx ->
        SchemaEvolution.add_column(schema, %{name: "email", type: "string"}, ctx)
      end)

      # Rename a column (doesn't need context)
      evolve_schema(table_path, fn schema, _ctx ->
        SchemaEvolution.rename_column(schema, "old_name", "new_name")
      end)
  """
  @spec evolve_schema(String.t(), function(), keyword()) ::
          {:ok, map()} | {:ok, map(), String.t()} | {:error, term()}
  def evolve_schema(table_path, evolution_fn, opts \\ []) do
    with {:ok, metadata} <- load(table_path, opts),
         current_schema <- get_current_schema(metadata) do
      # Build context for evolution operations
      context =
        opts
        |> Keyword.put(:next_field_id, metadata["last-column-id"] + 1)
        |> Keyword.put(:historical_schemas, get_historical_schemas(metadata))

      evolution_result = evolution_fn.(current_schema, context)

      case evolution_result do
        {:ok, evolved_schema} ->
          updated_metadata = apply_schema_evolution(metadata, evolved_schema)
          {:ok, updated_metadata}

        {:ok, evolved_schema, warnings} ->
          updated_metadata = apply_schema_evolution(metadata, evolved_schema)
          {:ok, updated_metadata, warnings}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Gets the current schema from metadata.

  ## Parameters
    - metadata: Table metadata map

  ## Returns
    Current schema map

  ## Examples

      iex> schema = Metadata.get_current_schema(metadata)
      iex> schema["schema-id"]
      0
  """
  @spec get_current_schema(map()) :: map()
  def get_current_schema(metadata) do
    current_schema_id = metadata["current-schema-id"]
    schemas = metadata["schemas"] || []

    Enum.find(schemas, fn schema ->
      schema["schema-id"] == current_schema_id
    end)
  end

  @doc """
  Saves metadata as v{N}.metadata.json and updates version-hint.text.

  ## Parameters
    - table_path: Relative path to table
    - metadata: Metadata to save
    - opts: Configuration options (storage, base_url, etc.)

  ## Returns
    `:ok` - Successfully saved
    `{:error, reason}` - Save failed
  """
  @spec save(String.t(), map(), keyword()) :: :ok | {:error, term()}
  def save(table_path, metadata, opts \\ []) do
    storage = Iceberg.Config.storage_backend(opts)
    version = metadata["last-sequence-number"] || 1
    metadata_path = "#{table_path}/metadata/v#{version}.metadata.json"
    metadata_json = JSON.encode!(metadata)

    with :ok <- storage.upload(metadata_path, metadata_json, content_type: "application/json"),
         :ok <- update_version_hint(table_path, version, opts) do
      Logger.info(fn -> "Saved metadata version #{version} for table: #{table_path}" end)
      :ok
    else
      {:error, reason} = error ->
        Logger.error(fn -> "Failed to save metadata for #{table_path}: #{inspect(reason)}" end)
        error
    end
  end

  @doc """
  Checks if a table exists by checking for metadata files.

  ## Parameters
    - table_path: Relative path to table
    - opts: Configuration options (storage, base_url, etc.)

  ## Returns
    `true` if table exists, `false` otherwise
  """
  @spec exists?(String.t(), keyword()) :: boolean()
  def exists?(table_path, opts \\ []) do
    match?({:ok, _version}, read_version_hint(table_path, opts))
  end

  @doc """
  Gets the source file from the latest snapshot's summary.
  Used for cursor-like incremental processing.

  ## Parameters
    - table_path: Relative path to table
    - opts: Configuration options (storage, base_url, etc.)

  ## Returns
    `{:ok, source_file}` - Source file from latest snapshot
    `{:ok, nil}` - No snapshots or no source_file in summary
    `{:error, reason}` - Load failed
  """
  @spec get_last_processed_file(String.t(), keyword()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def get_last_processed_file(table_path, opts \\ []) do
    with {:ok, metadata} <- load(table_path, opts) do
      {:ok, extract_source_file(metadata)}
    else
      {:error, :not_found} -> {:ok, nil}
      error -> error
    end
  end

  defp extract_source_file(metadata) do
    current_snapshot_id = metadata["current-snapshot-id"]

    if valid_snapshot_id?(current_snapshot_id) do
      metadata
      |> find_current_snapshot(current_snapshot_id)
      |> get_source_file_from_snapshot()
    else
      nil
    end
  end

  defp valid_snapshot_id?(nil), do: false
  defp valid_snapshot_id?(id) when id > 0, do: true
  defp valid_snapshot_id?(_), do: false

  defp find_current_snapshot(metadata, snapshot_id) do
    Enum.find(metadata["snapshots"] || [], fn s ->
      s["snapshot-id"] == snapshot_id
    end)
  end

  defp get_source_file_from_snapshot(%{"summary" => %{"source-file" => file}}), do: file
  defp get_source_file_from_snapshot(%{"summary" => %{"source_file" => file}}), do: file
  defp get_source_file_from_snapshot(_), do: nil

  ## Private Functions

  defp apply_schema_evolution(metadata, evolved_schema) do
    # Generate next schema ID
    next_schema_id = get_next_schema_id(metadata)

    # Add schema-id to evolved schema
    evolved_schema_with_id = Map.put(evolved_schema, "schema-id", next_schema_id)

    # Update last-column-id - use max of current last-column-id and max field ID in evolved schema
    # This ensures field IDs are never reused even after dropping columns
    current_last_column_id = metadata["last-column-id"] || 0
    max_field_id = get_max_field_id(evolved_schema)
    new_last_column_id = max(current_last_column_id, max_field_id)

    # Rebuild name mapping with new schema
    name_mapping = build_name_mapping(evolved_schema_with_id)

    metadata
    |> Map.put("current-schema-id", next_schema_id)
    |> Map.put("last-column-id", new_last_column_id)
    |> Map.put("last-updated-ms", System.system_time(:millisecond))
    |> Map.update("schemas", [evolved_schema_with_id], &(&1 ++ [evolved_schema_with_id]))
    |> Map.update("properties", %{}, fn props ->
      Map.put(props, "schema.name-mapping.default", name_mapping)
    end)
  end

  defp get_next_schema_id(metadata) do
    schemas = metadata["schemas"] || []

    if Enum.empty?(schemas) do
      0
    else
      max_id = schemas |> Enum.map(& &1["schema-id"]) |> Enum.max()
      max_id + 1
    end
  end

  defp get_max_field_id(schema) do
    fields = schema["fields"] || []

    if Enum.empty?(fields) do
      0
    else
      fields |> Enum.map(& &1["id"]) |> Enum.max()
    end
  end

  defp get_historical_schemas(metadata) do
    current_schema_id = metadata["current-schema-id"]
    schemas = metadata["schemas"] || []

    # Return all schemas except the current one
    Enum.reject(schemas, fn schema ->
      schema["schema-id"] == current_schema_id
    end)
  end

  defp read_version_hint(table_path, opts) do
    storage = Iceberg.Config.storage_backend(opts)

    case storage.download("#{table_path}/metadata/version-hint.text") do
      {:ok, content} ->
        version = content |> String.trim() |> String.to_integer()
        {:ok, version}

      {:error, _} ->
        # Check if v1.metadata.json exists (initial version)
        case storage.download("#{table_path}/metadata/v1.metadata.json") do
          {:ok, _} -> {:ok, 1}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp update_version_hint(table_path, version, opts) do
    storage = Iceberg.Config.storage_backend(opts)

    storage.upload(
      "#{table_path}/metadata/version-hint.text",
      "#{version}",
      content_type: "text/plain"
    )
  end

  defp count_fields(%{"fields" => fields}) when is_list(fields) do
    length(fields)
  end

  defp count_fields(_), do: 0

  defp count_partition_fields(%{"fields" => fields}) when is_list(fields) do
    length(fields)
  end

  defp count_partition_fields(_), do: 0

  # Builds the schema.name-mapping.default property for compute backend compatibility.
  # This maps column names to field IDs so engines can read Parquet files
  # that don't have embedded field IDs.
  #
  # Format per Iceberg spec:
  # [{"field-id": 1, "names": ["column_name"]}, ...]
  defp build_name_mapping(%{"fields" => fields}) when is_list(fields) do
    mapping =
      Enum.map(fields, fn field ->
        %{
          "field-id" => field["id"],
          "names" => [field["name"]]
        }
      end)

    JSON.encode!(mapping)
  end

  defp build_name_mapping(_schema), do: "[]"
end
