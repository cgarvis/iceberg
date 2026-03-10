defmodule Iceberg.SnapshotTest do
  use ExUnit.Case

  alias Iceberg.Snapshot
  alias Iceberg.Storage.Memory
  alias Iceberg.Test.MockCompute

  @opts [
    storage: Memory,
    compute: MockCompute,
    base_url: "memory://test",
    partition_spec: %{"spec-id" => 0, "fields" => []}
  ]

  setup do
    Memory.clear()
    MockCompute.clear()
    :ok
  end

  describe "create/4" do
    test "creates snapshot with required keys" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/file1.parquet",
            "file_size_in_bytes" => 1024,
            "record_count" => 100
          }
        ]
      })

      {:ok, snapshot} =
        Snapshot.create(:conn, "test/table", "memory://test/table/data/**/*.parquet", @opts)

      assert is_integer(snapshot["snapshot-id"])
      assert is_integer(snapshot["timestamp-ms"])
      assert is_binary(snapshot["manifest-list"])
      assert is_map(snapshot["summary"])
      assert snapshot["schema-id"] == 0
    end

    test "uploads manifest and manifest-list to storage" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/file1.parquet",
            "file_size_in_bytes" => 1024,
            "record_count" => 100
          }
        ]
      })

      {:ok, _snapshot} =
        Snapshot.create(:conn, "test/table", "memory://test/table/data/**/*.parquet", @opts)

      # Check that files were uploaded to storage
      files = Memory.dump()
      metadata_paths = Map.keys(files)

      # Should have at least one .avro manifest and one snap-*.avro manifest-list
      assert Enum.any?(metadata_paths, &String.ends_with?(&1, ".avro"))
      assert Enum.any?(metadata_paths, &String.contains?(&1, "snap-"))
    end

    test "summary includes operation, added-data-files, added-records, added-files-size" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/file1.parquet",
            "file_size_in_bytes" => 1024,
            "record_count" => 100
          },
          %{
            "file_path" => "memory://test/table/data/file2.parquet",
            "file_size_in_bytes" => 2048,
            "record_count" => 200
          }
        ]
      })

      opts = Keyword.put(@opts, :operation, "overwrite")

      {:ok, snapshot} =
        Snapshot.create(:conn, "test/table", "memory://test/table/data/**/*.parquet", opts)

      summary = snapshot["summary"]
      assert summary["operation"] == "overwrite"
      assert summary["added-data-files"] == "2"
      assert summary["added-records"] == "300"
      assert summary["added-files-size"] == "3072"
    end

    test "includes source-file in summary when provided" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "test.parquet",
            "file_size_in_bytes" => 100,
            "record_count" => 10
          }
        ]
      })

      opts = Keyword.put(@opts, :source_file, "sources/data.jsonl")
      {:ok, snapshot} = Snapshot.create(:conn, "test/table", "test/**/*.parquet", opts)

      assert snapshot["summary"]["source-file"] == "sources/data.jsonl"
    end

    test "upload_manifest sets correct added counts for multiple files" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/file1.parquet",
            "file_size_in_bytes" => 1024,
            "record_count" => 100
          },
          %{
            "file_path" => "memory://test/table/data/file2.parquet",
            "file_size_in_bytes" => 2048,
            "record_count" => 200
          }
        ]
      })

      {:ok, snapshot} =
        Snapshot.create(:conn, "test/table", "memory://test/table/data/**/*.parquet", @opts)

      # manifest-entries should reflect actual file counts
      manifest_entry = List.last(snapshot["manifest-entries"])
      assert manifest_entry.added_data_files_count == 1
    end

    test "uses explicit snapshot_id when provided" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "test.parquet",
            "file_size_in_bytes" => 100,
            "record_count" => 10
          }
        ]
      })

      opts = Keyword.put(@opts, :snapshot_id, 99_999)
      {:ok, snapshot} = Snapshot.create(:conn, "test/table", "test/**/*.parquet", opts)

      assert snapshot["snapshot-id"] == 99_999
    end
  end

  describe "create_replace/5" do
    test "creates replace snapshot with both added and deleted metrics" do
      # New files that will replace old ones
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/compacted.parquet",
            "file_size_in_bytes" => 3000,
            "record_count" => 300
          }
        ]
      })

      # Old files being replaced
      deleted_files = [
        %{
          file_path: "memory://test/table/data/file1.parquet",
          file_size_in_bytes: 1024,
          record_count: 100,
          partition_values: %{}
        },
        %{
          file_path: "memory://test/table/data/file2.parquet",
          file_size_in_bytes: 2048,
          record_count: 200,
          partition_values: %{}
        }
      ]

      {:ok, snapshot} =
        Snapshot.create_replace(
          :conn,
          "test/table",
          deleted_files,
          "memory://test/table/data/**/*.parquet",
          @opts
        )

      summary = snapshot["summary"]
      assert summary["operation"] == "replace"
      assert summary["added-data-files"] == "1"
      assert summary["added-records"] == "300"
      assert summary["added-files-size"] == "3000"
      assert summary["deleted-data-files"] == "2"
      assert summary["deleted-records"] == "300"
      assert summary["deleted-files-size"] == "3072"
    end

    test "manifest list includes both added and deleted manifests" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/compacted.parquet",
            "file_size_in_bytes" => 3000,
            "record_count" => 300
          }
        ]
      })

      deleted_files = [
        %{
          file_path: "memory://test/table/data/old.parquet",
          file_size_in_bytes: 1000,
          record_count: 100,
          partition_values: %{}
        }
      ]

      {:ok, snapshot} =
        Snapshot.create_replace(
          :conn,
          "test/table",
          deleted_files,
          "memory://test/table/data/**/*.parquet",
          @opts
        )

      # Should have 2 manifest entries (added + deleted)
      assert length(snapshot["manifest-entries"]) == 2

      [added_manifest, deleted_manifest] = snapshot["manifest-entries"]
      assert added_manifest.added_data_files_count == 1
      assert added_manifest.deleted_data_files_count == 0
      assert deleted_manifest.deleted_data_files_count == 1
      assert deleted_manifest.added_data_files_count == 0
    end

    test "carries forward parent manifests" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/table/data/compacted.parquet",
            "file_size_in_bytes" => 3000,
            "record_count" => 300
          }
        ]
      })

      parent_manifest = %{
        manifest_path: "memory://test/table/metadata/parent.avro",
        manifest_length: 500,
        partition_spec_id: 0,
        added_snapshot_id: 1000,
        added_data_files_count: 1,
        existing_data_files_count: 0,
        deleted_data_files_count: 0,
        added_rows_count: 50
      }

      deleted_files = [
        %{
          file_path: "memory://test/table/data/old.parquet",
          file_size_in_bytes: 1000,
          record_count: 100,
          partition_values: %{}
        }
      ]

      opts = Keyword.put(@opts, :parent_manifests, [parent_manifest])

      {:ok, snapshot} =
        Snapshot.create_replace(
          :conn,
          "test/table",
          deleted_files,
          "memory://test/table/data/**/*.parquet",
          opts
        )

      # Should have 3 manifest entries: parent + added + deleted
      assert length(snapshot["manifest-entries"]) == 3
    end
  end
end
