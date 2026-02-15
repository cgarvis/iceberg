defmodule Iceberg.ManifestList do
  @moduledoc """
  Creates Iceberg manifest-list files (Avro-encoded).

  A manifest-list is a snapshot's metadata file that lists all manifests
  and provides summary statistics for partition pruning.

  ## Example

      manifests = [
        %{
          manifest_path: "s3://bucket/table/metadata/manifest-1.avro",
          manifest_length: 5432,
          partition_spec_id: 0,
          added_snapshot_id: 1,
          added_data_files_count: 10,
          added_rows_count: 1000
        }
      ]

      {:ok, avro_binary} = Iceberg.ManifestList.create(manifests, 1, 1)
  """

  alias Iceberg.Avro.Encoder

  # Avro schema with field-id attributes per Iceberg v2 spec
  # See: https://iceberg.apache.org/spec/#manifest-lists
  @avro_schema %{
    "type" => "record",
    "name" => "manifest_file",
    "fields" => [
      %{"name" => "manifest_path", "type" => "string", "field-id" => 500},
      %{"name" => "manifest_length", "type" => "long", "field-id" => 501},
      %{"name" => "partition_spec_id", "type" => "int", "field-id" => 502},
      %{"name" => "content", "type" => "int", "field-id" => 517},
      %{"name" => "sequence_number", "type" => "long", "field-id" => 515},
      %{"name" => "min_sequence_number", "type" => "long", "field-id" => 516},
      %{"name" => "added_snapshot_id", "type" => "long", "field-id" => 503},
      %{"name" => "added_data_files_count", "type" => ["null", "int"], "field-id" => 504},
      %{"name" => "existing_data_files_count", "type" => ["null", "int"], "field-id" => 505},
      %{"name" => "deleted_data_files_count", "type" => ["null", "int"], "field-id" => 506},
      %{"name" => "added_rows_count", "type" => ["null", "long"], "field-id" => 512},
      %{"name" => "existing_rows_count", "type" => ["null", "long"], "field-id" => 513},
      %{"name" => "deleted_rows_count", "type" => ["null", "long"], "field-id" => 514},
      %{
        "name" => "partitions",
        "type" => [
          "null",
          %{
            "type" => "array",
            "items" => %{
              "type" => "record",
              "name" => "field_summary",
              "fields" => [
                %{"name" => "contains_null", "type" => "boolean", "field-id" => 509},
                %{"name" => "contains_nan", "type" => ["null", "boolean"], "field-id" => 518},
                %{"name" => "lower_bound", "type" => ["null", "bytes"], "field-id" => 510},
                %{"name" => "upper_bound", "type" => ["null", "bytes"], "field-id" => 511}
              ]
            },
            "element-id" => 508
          }
        ],
        "field-id" => 507
      },
      %{"name" => "key_metadata", "type" => ["null", "bytes"], "field-id" => 519}
    ]
  }

  @doc """
  Creates a manifest-list file from manifest metadata.

  ## Parameters
    - manifests: List of manifest file metadata
      - `:manifest_path` - Full S3 path to manifest file
      - `:manifest_length` - Manifest file size in bytes
      - `:partition_spec_id` - Partition spec ID (usually 0)
      - `:added_snapshot_id` - Snapshot ID that added this manifest
      - `:added_data_files_count` - Number of data files added
      - `:added_rows_count` - Number of rows added
    - snapshot_id: Snapshot ID for this manifest-list
    - sequence_number: Sequence number for this snapshot

  ## Returns
    `{:ok, binary}` - Avro-encoded manifest-list ready to upload
    `{:error, reason}` - Error during creation
  """
  @spec create(list(map()), integer(), integer()) :: {:ok, binary()} | {:error, term()}
  def create(manifests, snapshot_id, sequence_number) do
    manifest_entries =
      Enum.map(manifests, &build_manifest_entry(&1, snapshot_id, sequence_number))

    avro_binary = Encoder.encode(manifest_entries, @avro_schema)
    {:ok, avro_binary}
  rescue
    error -> {:error, error}
  end

  ## Private Functions

  defp build_manifest_entry(manifest, _snapshot_id, sequence_number) do
    %{
      "manifest_path" => get_field(manifest, :manifest_path),
      "manifest_length" => get_field(manifest, :manifest_length, 0),
      "partition_spec_id" => get_field(manifest, :partition_spec_id, 0),
      "content" => 0,
      # 0 = DATA
      "sequence_number" => sequence_number,
      "min_sequence_number" => sequence_number,
      "added_snapshot_id" => get_field(manifest, :added_snapshot_id, 0),
      "added_data_files_count" => get_field(manifest, :added_data_files_count, 0),
      "existing_data_files_count" => get_field(manifest, :existing_data_files_count, 0),
      "deleted_data_files_count" => get_field(manifest, :deleted_data_files_count, 0),
      "added_rows_count" => get_field(manifest, :added_rows_count, 0),
      "existing_rows_count" => get_field(manifest, :existing_rows_count, 0),
      "deleted_rows_count" => get_field(manifest, :deleted_rows_count, 0),
      "partitions" => get_field(manifest, :partitions),
      "key_metadata" => nil
    }
  end

  # Gets a field from a map, checking both atom and string keys
  defp get_field(map, key, default \\ nil) do
    map[key] || map[to_string(key)] || default
  end
end
