defmodule Iceberg.ManifestTest do
  use ExUnit.Case, async: true

  alias Iceberg.Manifest
  alias Iceberg.Test.AvroParser

  @partition_spec %{
    "spec-id" => 0,
    "fields" => [
      %{
        "name" => "date_day",
        "transform" => "day",
        "source-id" => 1,
        "field-id" => 1000
      }
    ]
  }

  @empty_partition_spec %{"spec-id" => 0, "fields" => []}

  describe "create/4" do
    test "output starts with Avro magic bytes" do
      data_files = [
        %{
          file_path: "s3://bucket/table/data/file1.parquet",
          file_size: 1024,
          record_count: 100,
          partition_values: %{}
        }
      ]

      {:ok, binary} = Manifest.create(data_files, 12_345, @empty_partition_spec)

      assert <<0x4F, 0x62, 0x6A, 0x01, _rest::binary>> = binary
    end

    test "header contains avro.schema with manifest_entry record" do
      data_files = [
        %{file_path: "test.parquet", file_size: 100, record_count: 10, partition_values: %{}}
      ]

      {:ok, binary} = Manifest.create(data_files, 1, @empty_partition_spec)
      {:ok, schema} = AvroParser.parse_schema(binary)

      assert schema["name"] == "manifest_entry"
      assert schema["type"] == "record"
    end

    test "header metadata includes format-version, partition-spec, partition-spec-id, schema-id" do
      data_files = [
        %{file_path: "test.parquet", file_size: 100, record_count: 10, partition_values: %{}}
      ]

      {:ok, binary} = Manifest.create(data_files, 1, @empty_partition_spec)
      {:ok, metadata} = AvroParser.parse_metadata(binary)

      assert metadata["format-version"] == "2"
      assert metadata["partition-spec-id"] == "0"
      assert metadata["schema-id"] == "0"
      assert is_binary(metadata["partition-spec"])
    end

    test "field IDs in manifest schema match Iceberg v2 spec" do
      data_files = [
        %{file_path: "test.parquet", file_size: 100, record_count: 10, partition_values: %{}}
      ]

      {:ok, binary} = Manifest.create(data_files, 1, @empty_partition_spec)
      {:ok, schema} = AvroParser.parse_schema(binary)

      fields = schema["fields"]

      # Top-level fields
      status_field = Enum.find(fields, &(&1["name"] == "status"))
      assert status_field["field-id"] == 0

      snapshot_id_field = Enum.find(fields, &(&1["name"] == "snapshot_id"))
      assert snapshot_id_field["field-id"] == 1

      data_file_field = Enum.find(fields, &(&1["name"] == "data_file"))
      assert data_file_field["field-id"] == 2

      # data_file sub-fields
      df_fields = data_file_field["type"]["fields"]

      file_path_field = Enum.find(df_fields, &(&1["name"] == "file_path"))
      assert file_path_field["field-id"] == 100

      file_format_field = Enum.find(df_fields, &(&1["name"] == "file_format"))
      assert file_format_field["field-id"] == 101

      partition_field = Enum.find(df_fields, &(&1["name"] == "partition"))
      assert partition_field["field-id"] == 102

      record_count_field = Enum.find(df_fields, &(&1["name"] == "record_count"))
      assert record_count_field["field-id"] == 103

      file_size_field = Enum.find(df_fields, &(&1["name"] == "file_size_in_bytes"))
      assert file_size_field["field-id"] == 104
    end

    test "includes table schema in header when provided" do
      data_files = [
        %{file_path: "test.parquet", file_size: 100, record_count: 10, partition_values: %{}}
      ]

      table_schema = %{
        "type" => "record",
        "schema-id" => 0,
        "fields" => [%{"id" => 1, "name" => "id", "type" => "string"}]
      }

      {:ok, binary} =
        Manifest.create(data_files, 1, @empty_partition_spec, table_schema: table_schema)

      {:ok, metadata} = AvroParser.parse_metadata(binary)
      assert is_binary(metadata["schema"])
      decoded_schema = JSON.decode!(metadata["schema"])
      assert decoded_schema["type"] == "record"
    end

    test "partition values handled correctly for day transform" do
      data_files = [
        %{
          file_path: "test.parquet",
          file_size: 100,
          record_count: 10,
          partition_values: %{"date_day" => 19_738}
        }
      ]

      {:ok, binary} = Manifest.create(data_files, 1, @partition_spec)

      # Should produce valid Avro output
      assert <<0x4F, 0x62, 0x6A, 0x01, _rest::binary>> = binary
    end

    test "partition values handled correctly for identity transform" do
      identity_spec = %{
        "spec-id" => 0,
        "fields" => [
          %{
            "name" => "category",
            "transform" => "identity",
            "source-id" => 1,
            "field-id" => 1000
          }
        ]
      }

      data_files = [
        %{
          file_path: "test.parquet",
          file_size: 100,
          record_count: 10,
          partition_values: %{"category" => "events"}
        }
      ]

      {:ok, binary} = Manifest.create(data_files, 1, identity_spec)
      assert <<0x4F, 0x62, 0x6A, 0x01, _rest::binary>> = binary
    end
  end
end
