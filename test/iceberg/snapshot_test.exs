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

      opts = Keyword.put(@opts, :snapshot_id, 99999)
      {:ok, snapshot} = Snapshot.create(:conn, "test/table", "test/**/*.parquet", opts)

      assert snapshot["snapshot-id"] == 99999
    end
  end
end
