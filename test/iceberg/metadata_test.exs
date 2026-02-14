defmodule Iceberg.MetadataTest do
  use ExUnit.Case

  alias Iceberg.Metadata
  alias Iceberg.Storage.Memory

  @opts [storage: Memory, base_url: "memory://test"]

  setup do
    Memory.clear()
    :ok
  end

  describe "create_initial/4" do
    test "produces all required v2 metadata fields" do
      schema = %{
        "type" => "record",
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "required" => true, "type" => "string"},
          %{"id" => 2, "name" => "name", "required" => false, "type" => "string"}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}

      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)

      # All 17 required v2 metadata fields
      assert metadata["format-version"] == 2
      assert is_binary(metadata["table-uuid"])
      assert is_binary(metadata["location"])
      assert metadata["last-sequence-number"] == 0
      assert is_integer(metadata["last-updated-ms"])
      assert metadata["last-column-id"] == 2
      assert is_list(metadata["schemas"])
      assert metadata["current-schema-id"] == 0
      assert is_list(metadata["partition-specs"])
      assert metadata["default-spec-id"] == 0
      assert is_integer(metadata["last-partition-id"])
      assert is_map(metadata["properties"])
      assert metadata["current-snapshot-id"] == -1
      assert metadata["snapshots"] == []
      assert metadata["snapshot-log"] == []
      assert metadata["metadata-log"] == []
      assert is_list(metadata["sort-orders"])
      assert metadata["default-sort-order-id"] == 0
    end

    test "format-version is 2" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      assert metadata["format-version"] == 2
    end

    test "current-snapshot-id is -1" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      assert metadata["current-snapshot-id"] == -1
    end

    test "snapshots is empty list" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      assert metadata["snapshots"] == []
    end

    test "UUID is valid v4 format" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      uuid = metadata["table-uuid"]

      assert Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               uuid
             )
    end

    test "includes name mapping in properties" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id"},
          %{"id" => 2, "name" => "name"}
        ]
      }

      {:ok, metadata} =
        Metadata.create_initial("test/table", schema, %{"fields" => []}, @opts)

      name_mapping = metadata["properties"]["schema.name-mapping.default"]
      assert is_binary(name_mapping)
      decoded = JSON.decode!(name_mapping)
      assert length(decoded) == 2
      assert Enum.at(decoded, 0)["field-id"] == 1
      assert Enum.at(decoded, 0)["names"] == ["id"]
    end
  end

  describe "save/3 and load/2" do
    test "round-trips through storage" do
      schema = %{
        "type" => "record",
        "schema-id" => 0,
        "fields" => [%{"id" => 1, "name" => "id", "required" => true, "type" => "string"}]
      }

      {:ok, metadata} =
        Metadata.create_initial("test/table", schema, %{"spec-id" => 0, "fields" => []}, @opts)

      # Save sets version to last-sequence-number (0), but the save function uses it
      # Initial save will use version 0
      :ok = Metadata.save("test/table", metadata, @opts)

      # Load should return the same metadata
      {:ok, loaded} = Metadata.load("test/table", @opts)

      assert loaded["format-version"] == metadata["format-version"]
      assert loaded["table-uuid"] == metadata["table-uuid"]
      assert loaded["current-snapshot-id"] == metadata["current-snapshot-id"]
    end

    test "version-hint.text contains correct version number" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      :ok = Metadata.save("test/table", metadata, @opts)

      {:ok, hint} = Memory.download("test/table/metadata/version-hint.text")
      assert String.trim(hint) == "0"
    end
  end

  describe "add_snapshot/2" do
    test "increments sequence number" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => System.system_time(:millisecond),
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert updated["last-sequence-number"] == 1
    end

    test "updates current-snapshot-id" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => System.system_time(:millisecond),
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert updated["current-snapshot-id"] == 12_345
    end

    test "appends to snapshot-log" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => 1000,
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert length(updated["snapshot-log"]) == 1
      [log_entry] = updated["snapshot-log"]
      assert log_entry["snapshot-id"] == 12_345
      assert log_entry["timestamp-ms"] == 1000
    end

    test "appends snapshot to snapshots list" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => 1000,
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert length(updated["snapshots"]) == 1
      assert List.first(updated["snapshots"])["snapshot-id"] == 12_345
    end
  end

  describe "exists?/2" do
    test "returns false for non-existent table" do
      refute Metadata.exists?("nonexistent/table", @opts)
    end

    test "returns true for existing table" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      :ok = Metadata.save("test/table", metadata, @opts)

      assert Metadata.exists?("test/table", @opts)
    end
  end

  describe "get_last_processed_file/2" do
    test "returns nil for table with no snapshots" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      :ok = Metadata.save("test/table", metadata, @opts)

      assert {:ok, nil} = Metadata.get_last_processed_file("test/table", @opts)
    end

    test "extracts source-file from summary" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => 1000,
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append", "source-file" => "sources/data.jsonl"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)
      :ok = Metadata.save("test/table", updated, @opts)

      assert {:ok, "sources/data.jsonl"} =
               Metadata.get_last_processed_file("test/table", @opts)
    end

    test "returns nil for non-existent table" do
      assert {:ok, nil} = Metadata.get_last_processed_file("nonexistent", @opts)
    end
  end
end
