defmodule Iceberg.Manifest do
  @moduledoc """
  Creates Iceberg manifest files (Avro-encoded).

  A manifest file lists data files with metadata:
  - File path, size, record count
  - Partition values
  - Column statistics (null counts, bounds)
  - File format (Parquet)

  Manifests are encoded as Avro Object Container Files following the
  Apache Iceberg v2 specification.

  ## Example

      data_files = [
        %{
          file_path: "s3://bucket/table/data/file1.parquet",
          file_size: 1024,
          record_count: 100,
          partition_values: %{"date" => "2024-01-15"}
        }
      ]

      partition_spec = %{
        spec_id: 0,
        fields: [
          %{name: "date_partition", transform: "day", source_id: 1}
        ]
      }

      {:ok, avro_binary} = Iceberg.Manifest.create(data_files, 1, partition_spec)
  """

  alias Iceberg.Avro.Encoder
  alias Iceberg.SingleValue

  @doc """
  Creates a manifest file from a list of data files.

  ## Parameters
    - data_files: List of Parquet file metadata
      - `:file_path` - Full S3 path to Parquet file
      - `:file_size` - File size in bytes
      - `:record_count` - Number of records
      - `:partition_values` - Map of partition column => value
      - `:column_stats` - Optional column statistics
    - snapshot_id: Snapshot ID for this manifest
    - partition_spec: Partition specification
      - `:spec_id` - Partition spec ID (usually 0)
      - `:fields` - List of partition fields
    - opts: Options
      - `:table_schema` - The table schema (required for v2 format)
      - `:schema_id` - Schema ID (default: 0)
      - `:format_version` - Iceberg format version (default: 2)

  ## Returns
    `{:ok, binary}` - Avro-encoded manifest ready to upload
    `{:error, reason}` - Error during creation
  """
  @spec create(list(map()), integer(), map(), keyword()) :: {:ok, binary()} | {:error, term()}
  def create(data_files, snapshot_id, partition_spec, opts \\ []) do
    avro_schema = build_manifest_schema(partition_spec)
    table_schema = Keyword.get(opts, :table_schema)
    schema_id = Keyword.get(opts, :schema_id, 0)
    format_version = Keyword.get(opts, :format_version, 2)

    manifest_entries =
      Enum.map(data_files, &build_manifest_entry(&1, snapshot_id, partition_spec, table_schema))

    # Build Iceberg-specific Avro file metadata per the spec:
    # https://iceberg.apache.org/spec/#manifests
    iceberg_metadata =
      build_iceberg_metadata(partition_spec, table_schema, schema_id, format_version)

    avro_binary = Encoder.encode(manifest_entries, avro_schema, metadata: iceberg_metadata)
    {:ok, avro_binary}
  rescue
    error -> {:error, error}
  end

  # Builds the Iceberg-specific metadata for the Avro file header
  # Required properties: schema, schema-id, partition-spec, partition-spec-id, format-version
  defp build_iceberg_metadata(partition_spec, table_schema, schema_id, format_version) do
    spec_id = partition_spec["spec-id"] || partition_spec[:spec_id] || 0
    fields = get_partition_fields(partition_spec)

    metadata = %{
      "format-version" => to_string(format_version),
      "partition-spec-id" => to_string(spec_id),
      "partition-spec" => JSON.encode!(fields),
      "schema-id" => to_string(schema_id)
    }

    # Add table schema if provided
    if table_schema do
      Map.put(metadata, "schema", JSON.encode!(table_schema))
    else
      metadata
    end
  end

  ## Private Functions

  # Builds the Avro schema for manifest entries with field-id attributes per Iceberg v2 spec
  # See: https://iceberg.apache.org/spec/#manifests
  defp build_manifest_schema(partition_spec) do
    partition_record_schema = build_partition_schema(partition_spec)

    %{
      "type" => "record",
      "name" => "manifest_entry",
      "fields" => [
        %{"name" => "status", "type" => "int", "field-id" => 0},
        %{"name" => "snapshot_id", "type" => ["null", "long"], "field-id" => 1},
        %{"name" => "sequence_number", "type" => ["null", "long"], "field-id" => 3},
        %{"name" => "file_sequence_number", "type" => ["null", "long"], "field-id" => 4},
        %{
          "name" => "data_file",
          "type" => %{
            "type" => "record",
            "name" => "data_file",
            "fields" => [
              %{"name" => "content", "type" => "int", "field-id" => 134},
              %{"name" => "file_path", "type" => "string", "field-id" => 100},
              %{"name" => "file_format", "type" => "string", "field-id" => 101},
              %{"name" => "partition", "type" => partition_record_schema, "field-id" => 102},
              %{"name" => "record_count", "type" => "long", "field-id" => 103},
              %{"name" => "file_size_in_bytes", "type" => "long", "field-id" => 104},
              %{
                "name" => "column_sizes",
                "type" => [
                  "null",
                  %{
                    "type" => "array",
                    "logicalType" => "map",
                    "items" => %{
                      "type" => "record",
                      "name" => "k117_v118",
                      "fields" => [
                        %{"name" => "key", "type" => "int", "field-id" => 117},
                        %{"name" => "value", "type" => "long", "field-id" => 118}
                      ]
                    },
                    "element-id" => 108
                  }
                ],
                "field-id" => 108
              },
              %{
                "name" => "value_counts",
                "type" => [
                  "null",
                  %{
                    "type" => "array",
                    "logicalType" => "map",
                    "items" => %{
                      "type" => "record",
                      "name" => "k119_v120",
                      "fields" => [
                        %{"name" => "key", "type" => "int", "field-id" => 119},
                        %{"name" => "value", "type" => "long", "field-id" => 120}
                      ]
                    },
                    "element-id" => 109
                  }
                ],
                "field-id" => 109
              },
              %{
                "name" => "null_value_counts",
                "type" => [
                  "null",
                  %{
                    "type" => "array",
                    "logicalType" => "map",
                    "items" => %{
                      "type" => "record",
                      "name" => "k121_v122",
                      "fields" => [
                        %{"name" => "key", "type" => "int", "field-id" => 121},
                        %{"name" => "value", "type" => "long", "field-id" => 122}
                      ]
                    },
                    "element-id" => 110
                  }
                ],
                "field-id" => 110
              },
              %{
                "name" => "nan_value_counts",
                "type" => [
                  "null",
                  %{
                    "type" => "array",
                    "logicalType" => "map",
                    "items" => %{
                      "type" => "record",
                      "name" => "k138_v139",
                      "fields" => [
                        %{"name" => "key", "type" => "int", "field-id" => 138},
                        %{"name" => "value", "type" => "long", "field-id" => 139}
                      ]
                    },
                    "element-id" => 137
                  }
                ],
                "field-id" => 137
              },
              %{
                "name" => "lower_bounds",
                "type" => [
                  "null",
                  %{
                    "type" => "array",
                    "logicalType" => "map",
                    "items" => %{
                      "type" => "record",
                      "name" => "k126_v127",
                      "fields" => [
                        %{"name" => "key", "type" => "int", "field-id" => 126},
                        %{"name" => "value", "type" => "bytes", "field-id" => 127}
                      ]
                    },
                    "element-id" => 125
                  }
                ],
                "field-id" => 125
              },
              %{
                "name" => "upper_bounds",
                "type" => [
                  "null",
                  %{
                    "type" => "array",
                    "logicalType" => "map",
                    "items" => %{
                      "type" => "record",
                      "name" => "k129_v130",
                      "fields" => [
                        %{"name" => "key", "type" => "int", "field-id" => 129},
                        %{"name" => "value", "type" => "bytes", "field-id" => 130}
                      ]
                    },
                    "element-id" => 128
                  }
                ],
                "field-id" => 128
              },
              %{"name" => "key_metadata", "type" => ["null", "bytes"], "field-id" => 131},
              %{
                "name" => "split_offsets",
                "type" => [
                  "null",
                  %{"type" => "array", "items" => "long", "element-id" => 133}
                ],
                "field-id" => 132
              },
              %{
                "name" => "equality_ids",
                "type" => [
                  "null",
                  %{"type" => "array", "items" => "int", "element-id" => 136}
                ],
                "field-id" => 135
              },
              %{"name" => "sort_order_id", "type" => ["null", "int"], "field-id" => 140}
            ]
          },
          "field-id" => 2
        }
      ]
    }
  end

  # Builds the partition schema based on partition spec
  # Handles both atom keys (:fields) and string keys ("fields")
  defp build_partition_schema(partition_spec) do
    fields = get_partition_fields(partition_spec)
    partition_fields = build_partition_fields(fields)

    %{
      "type" => "record",
      "name" => "r102",
      "fields" => partition_fields
    }
  end

  defp build_partition_fields([]), do: []

  defp build_partition_fields(fields) when is_list(fields) do
    Enum.map(fields, &build_partition_field/1)
  end

  # Gets a field value from a map, checking multiple keys
  defp get_field_value(map, keys, default \\ nil) when is_list(keys) do
    Enum.find_value(keys, default, &Map.get(map, &1))
  end

  defp build_partition_field(field) do
    field_name = get_field_value(field, [:name, "name"], "partition_#{get_source_id(field)}")
    field_id = get_field_value(field, [:"field-id", "field-id"])
    field_type = partition_type_for_transform(get_field_value(field, [:transform, "transform"]))

    base = %{"name" => field_name, "type" => avro_type(field_type)}
    if field_id, do: Map.put(base, "field-id", field_id), else: base
  end

  defp get_source_id(field) do
    get_field_value(field, [:source_id, "source-id"])
  end

  # Returns the Iceberg type for partition values based on transform
  # See: https://iceberg.apache.org/spec/#partition-transforms
  defp partition_type_for_transform("day"), do: "int"
  defp partition_type_for_transform("month"), do: "int"
  defp partition_type_for_transform("year"), do: "int"
  defp partition_type_for_transform("hour"), do: "int"
  defp partition_type_for_transform("bucket"), do: "int"
  defp partition_type_for_transform("truncate"), do: "string"
  defp partition_type_for_transform("identity"), do: "string"
  defp partition_type_for_transform(_), do: "string"

  defp get_partition_fields(%{fields: fields}), do: fields
  defp get_partition_fields(%{"fields" => fields}), do: fields
  defp get_partition_fields(_), do: []

  # Builds a single manifest entry
  defp build_manifest_entry(data_file, snapshot_id, partition_spec, table_schema) do
    partition_values = extract_partition_values(data_file, partition_spec)

    # Encode bounds as binary data according to Iceberg single-value serialization
    lower_bounds = encode_bounds(data_file[:lower_bounds], table_schema)
    upper_bounds = encode_bounds(data_file[:upper_bounds], table_schema)

    %{
      "status" => 1,
      # 1 = ADDED
      "snapshot_id" => snapshot_id,
      "sequence_number" => nil,
      "file_sequence_number" => nil,
      "data_file" => %{
        "content" => 0,
        # 0 = DATA
        "file_path" => data_file[:file_path] || data_file["file_path"],
        "file_format" => "PARQUET",
        "partition" => partition_values,
        "record_count" => data_file[:record_count] || data_file["record_count"] || 0,
        "file_size_in_bytes" =>
          data_file[:file_size_in_bytes] || data_file[:file_size] || data_file["file_size"] || 0,
        "column_sizes" => data_file[:column_sizes],
        "value_counts" => data_file[:value_counts],
        "null_value_counts" => data_file[:null_value_counts],
        "nan_value_counts" => nil,
        "lower_bounds" => lower_bounds,
        "upper_bounds" => upper_bounds,
        "key_metadata" => nil,
        "split_offsets" => nil,
        "equality_ids" => nil,
        "sort_order_id" => nil
      }
    }
  end

  # Encodes bounds map using SingleValue serialization
  defp encode_bounds(bounds, table_schema) do
    if table_schema && bounds do
      SingleValue.encode_bounds_map(bounds, table_schema)
    else
      bounds
    end
  end

  # Extracts partition values from data file metadata
  # Handles both atom keys (:fields) and string keys ("fields")
  defp extract_partition_values(data_file, partition_spec) do
    fields = get_partition_fields(partition_spec)

    if has_partition_fields?(fields) do
      partition_data = get_field_value(data_file, [:partition_values, "partition_values"], %{})
      extract_field_values(fields, partition_data)
    else
      %{}
    end
  end

  defp has_partition_fields?([]), do: false
  defp has_partition_fields?(fields) when is_list(fields), do: true
  defp has_partition_fields?(_), do: false

  defp extract_field_values(fields, partition_data) do
    Enum.reduce(fields, %{}, fn field, acc ->
      field_name = get_field_value(field, [:name, "name"], "partition_#{get_source_id(field)}")
      transform = get_field_value(field, [:transform, "transform"])
      raw_value = get_partition_data_value(partition_data, field_name)
      value = convert_partition_value(raw_value, transform, partition_data)

      Map.put(acc, field_name, value)
    end)
  end

  defp get_partition_data_value(partition_data, field_name) do
    Map.get(partition_data, field_name) || Map.get(partition_data, to_string(field_name))
  end

  # Converts partition value to appropriate type based on transform
  # For day transform, computes days since epoch from year/month/day if available
  defp convert_partition_value(nil, "day", partition_data) do
    # Try to compute from year/month/day hive partitions
    year = get_int_value(partition_data, "year")
    month = get_int_value(partition_data, "month")
    day = get_int_value(partition_data, "day")

    if year && month && day do
      # Calculate days since epoch (1970-01-01)
      date = Date.new!(year, month, day)
      Date.diff(date, ~D[1970-01-01])
    else
      0
    end
  end

  defp convert_partition_value(value, "day", _partition_data) when is_integer(value), do: value

  defp convert_partition_value(value, "day", _partition_data) when is_binary(value),
    do: String.to_integer(value)

  defp convert_partition_value(value, "month", _partition_data) when is_integer(value), do: value

  defp convert_partition_value(value, "month", _partition_data) when is_binary(value),
    do: String.to_integer(value)

  defp convert_partition_value(value, "year", _partition_data) when is_integer(value), do: value

  defp convert_partition_value(value, "year", _partition_data) when is_binary(value),
    do: String.to_integer(value)

  defp convert_partition_value(value, _transform, _partition_data), do: value

  defp get_int_value(map, key) do
    case Map.get(map, key) || Map.get(map, String.to_atom(key)) do
      nil -> nil
      val when is_integer(val) -> val
      val when is_binary(val) -> String.to_integer(val)
      _ -> nil
    end
  end

  # Maps Iceberg types to Avro types
  defp avro_type("string"), do: "string"
  defp avro_type("int"), do: "int"
  defp avro_type("long"), do: "long"
  defp avro_type("date"), do: "int"
  defp avro_type("timestamp"), do: "long"
  defp avro_type(_), do: "string"
end
