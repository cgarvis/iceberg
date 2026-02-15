defmodule Iceberg.Snapshot do
  @moduledoc """
  Creates Iceberg snapshots with Avro-encoded manifests.

  Orchestrates the full snapshot creation workflow:
  1. Extract Parquet statistics from data files
  2. Create manifest file (Avro-encoded)
  3. Upload manifest to storage
  4. Create manifest-list file (Avro-encoded)
  5. Upload manifest-list to storage
  6. Return snapshot metadata
  """

  alias Iceberg.{Error, Manifest, ManifestList, ParquetStats, UUID}
  require Logger

  @doc """
  Creates a new snapshot from data files.

  ## Parameters
    - conn: Compute backend connection
    - table_path: Relative Iceberg table path
    - data_file_pattern: Glob pattern for data files
    - opts: Options
      - `:partition_spec` - Partition specification (required)
      - `:operation` - Operation type ("append", "overwrite", etc.)
      - `:source_file` - Source file identifier for lineage
      - `:snapshot_id` - Explicit snapshot ID (default: generated)
      - `:sequence_number` - Sequence number (default: 1)

  ## Returns
    `{:ok, snapshot_metadata}` - Snapshot metadata for v{N}.metadata.json
    `{:error, reason}` - Snapshot creation failed
  """
  @spec create(term(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create(conn, table_path, data_file_pattern, opts \\ []) do
    partition_spec = Keyword.fetch!(opts, :partition_spec)
    snapshot_id = Keyword.get(opts, :snapshot_id, generate_snapshot_id())
    sequence_number = Keyword.get(opts, :sequence_number, 1)
    operation = Keyword.get(opts, :operation, "append")
    source_file = Keyword.get(opts, :source_file)
    table_schema = Keyword.get(opts, :table_schema)
    schema_id = Keyword.get(opts, :schema_id, 0)

    Logger.info(fn -> "Creating snapshot #{snapshot_id} for table: #{table_path}" end)

    manifest_opts = [table_schema: table_schema, schema_id: schema_id]

    with {:ok, stats} <- ParquetStats.extract(conn, data_file_pattern, opts),
         {:ok, manifest_avro} <-
           Manifest.create(stats, snapshot_id, partition_spec, manifest_opts),
         {:ok, manifest_metadata} <-
           upload_manifest(manifest_avro, table_path, snapshot_id, opts),
         {:ok, manifest_list_avro} <-
           ManifestList.create([manifest_metadata], snapshot_id, sequence_number),
         {:ok, manifest_list_path} <-
           upload_manifest_list(manifest_list_avro, table_path, snapshot_id, opts) do
      snapshot =
        build_snapshot_metadata(
          snapshot_id,
          manifest_list_path,
          stats,
          operation,
          source_file
        )

      Logger.info(fn ->
        "Created snapshot #{snapshot_id}: #{length(stats)} files, #{total_records(stats)} records"
      end)

      {:ok, snapshot}
    else
      {:error, reason} = error ->
        Logger.error(fn -> "Failed to create snapshot #{snapshot_id}: #{inspect(reason)}" end)
        error
    end
  end

  ## Private Functions

  defp upload_manifest(avro_binary, table_path, snapshot_id, opts) do
    storage = Iceberg.Config.storage_backend(opts)
    manifest_id = UUID.generate()
    relative_path = "#{table_path}/metadata/#{manifest_id}.avro"
    full_path = Iceberg.Config.full_url(relative_path, opts)

    case storage.upload(relative_path, avro_binary, content_type: "application/octet-stream") do
      :ok ->
        manifest_length = byte_size(avro_binary)

        metadata = %{
          manifest_path: full_path,
          manifest_length: manifest_length,
          partition_spec_id: 0,
          added_snapshot_id: snapshot_id,
          added_data_files_count: 1,
          existing_data_files_count: 0,
          deleted_data_files_count: 0,
          added_rows_count: 0
        }

        Logger.debug(fn -> "Uploaded manifest: #{relative_path} (#{manifest_length} bytes)" end)
        {:ok, metadata}

      {:error, reason} ->
        Error.manifest_upload_failed(reason)
    end
  end

  defp upload_manifest_list(avro_binary, table_path, snapshot_id, opts) do
    storage = Iceberg.Config.storage_backend(opts)
    manifest_list_id = UUID.generate()
    relative_path = "#{table_path}/metadata/snap-#{snapshot_id}-#{manifest_list_id}.avro"
    full_path = Iceberg.Config.full_url(relative_path, opts)

    case storage.upload(relative_path, avro_binary, content_type: "application/octet-stream") do
      :ok ->
        Logger.debug(fn ->
          "Uploaded manifest-list: #{relative_path} (#{byte_size(avro_binary)} bytes)"
        end)

        {:ok, full_path}

      {:error, reason} ->
        Error.manifest_list_upload_failed(reason)
    end
  end

  defp build_snapshot_metadata(snapshot_id, manifest_list_path, stats, operation, source_file) do
    total_files = length(stats)
    total_rows = total_records(stats)
    total_bytes = Enum.sum(Enum.map(stats, & &1[:file_size_in_bytes]))

    summary = %{
      "operation" => operation,
      "added-data-files" => to_string(total_files),
      "added-records" => to_string(total_rows),
      "added-files-size" => to_string(total_bytes)
    }

    summary =
      if source_file do
        Map.put(summary, "source-file", source_file)
      else
        summary
      end

    %{
      "snapshot-id" => snapshot_id,
      "timestamp-ms" => System.system_time(:millisecond),
      "manifest-list" => manifest_list_path,
      "summary" => summary,
      "schema-id" => 0
    }
  end

  defp generate_snapshot_id do
    # Use millisecond timestamp as snapshot ID
    System.system_time(:millisecond)
  end

  defp total_records(stats) do
    Enum.sum(Enum.map(stats, & &1[:record_count]))
  end
end
