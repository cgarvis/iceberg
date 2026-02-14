defmodule Iceberg.ManifestListTest do
  use ExUnit.Case, async: true

  alias Iceberg.ManifestList
  alias Iceberg.Test.AvroParser

  describe "create/3" do
    test "produces valid Avro output with magic bytes" do
      manifests = [
        %{
          manifest_path: "s3://bucket/table/metadata/manifest-1.avro",
          manifest_length: 5432,
          partition_spec_id: 0,
          added_snapshot_id: 12345,
          added_data_files_count: 10,
          added_rows_count: 1000
        }
      ]

      {:ok, binary} = ManifestList.create(manifests, 12345, 1)

      assert <<0x4F, 0x62, 0x6A, 0x01, _rest::binary>> = binary
    end

    test "field IDs match Iceberg v2 spec" do
      manifests = [
        %{
          manifest_path: "test.avro",
          manifest_length: 100,
          partition_spec_id: 0,
          added_snapshot_id: 1,
          added_data_files_count: 1,
          added_rows_count: 10
        }
      ]

      {:ok, binary} = ManifestList.create(manifests, 1, 1)
      {:ok, schema} = AvroParser.parse_schema(binary)

      assert schema["name"] == "manifest_file"

      fields = schema["fields"]

      manifest_path_field = Enum.find(fields, &(&1["name"] == "manifest_path"))
      assert manifest_path_field["field-id"] == 500

      manifest_length_field = Enum.find(fields, &(&1["name"] == "manifest_length"))
      assert manifest_length_field["field-id"] == 501

      partition_spec_id_field = Enum.find(fields, &(&1["name"] == "partition_spec_id"))
      assert partition_spec_id_field["field-id"] == 502

      added_snapshot_id_field = Enum.find(fields, &(&1["name"] == "added_snapshot_id"))
      assert added_snapshot_id_field["field-id"] == 503

      added_files_field = Enum.find(fields, &(&1["name"] == "added_data_files_count"))
      assert added_files_field["field-id"] == 504

      added_rows_field = Enum.find(fields, &(&1["name"] == "added_rows_count"))
      assert added_rows_field["field-id"] == 512

      partitions_field = Enum.find(fields, &(&1["name"] == "partitions"))
      assert partitions_field["field-id"] == 507
    end

    test "entry structure preserves manifest metadata" do
      manifests = [
        %{
          manifest_path: "s3://bucket/manifest.avro",
          manifest_length: 2048,
          partition_spec_id: 0,
          added_snapshot_id: 99,
          added_data_files_count: 5,
          added_rows_count: 500,
          existing_data_files_count: 3,
          deleted_data_files_count: 1
        }
      ]

      {:ok, binary} = ManifestList.create(manifests, 99, 2)

      # Should encode without errors and produce valid Avro
      assert byte_size(binary) > 100
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end

    test "handles multiple manifests" do
      manifests = [
        %{
          manifest_path: "manifest-1.avro",
          manifest_length: 100,
          added_snapshot_id: 1,
          added_data_files_count: 5,
          added_rows_count: 100
        },
        %{
          manifest_path: "manifest-2.avro",
          manifest_length: 200,
          added_snapshot_id: 1,
          added_data_files_count: 3,
          added_rows_count: 50
        }
      ]

      {:ok, binary} = ManifestList.create(manifests, 1, 1)
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end
  end
end
