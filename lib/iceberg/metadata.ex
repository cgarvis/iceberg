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
        Logger.debug("Metadata not found for table: #{table_path}")
        error

      {:error, reason} = error ->
        Logger.error("Failed to load metadata for #{table_path}: #{inspect(reason)}")
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
      "table-uuid" => generate_uuid(),
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
      Logger.info("Saved metadata version #{version} for table: #{table_path}")
      :ok
    else
      {:error, reason} = error ->
        Logger.error("Failed to save metadata for #{table_path}: #{inspect(reason)}")
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
    case read_version_hint(table_path, opts) do
      {:ok, _version} -> true
      {:error, _} -> false
    end
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
    case load(table_path, opts) do
      {:ok, metadata} -> {:ok, extract_source_file(metadata)}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
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

  defp generate_uuid do
    # Generate UUID v4
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)

    <<u0::48, 4::4, u1::12, 2::2, u2::62>>
    |> Base.encode16(case: :lower)
    |> String.replace(~r/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, "\\1-\\2-\\3-\\4-\\5")
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
