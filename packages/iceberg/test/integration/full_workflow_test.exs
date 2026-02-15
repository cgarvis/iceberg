defmodule Iceberg.Integration.FullWorkflowTest do
  use ExUnit.Case

  alias Iceberg.{Metadata, Table}
  alias Iceberg.Storage.Memory
  alias Iceberg.Test.MockCompute

  @opts [
    storage: Memory,
    compute: MockCompute,
    base_url: "memory://test"
  ]

  setup do
    Memory.clear()
    MockCompute.clear()
    :ok
  end

  describe "full workflow" do
    test "create table -> verify metadata -> simulate snapshot -> verify updated metadata" do
      # Step 1: Create table
      :ok = Table.create(Iceberg.Test.Schemas.Events, nil, @opts)

      # Step 2: Verify initial metadata
      {:ok, metadata} = Metadata.load("canonical/events", @opts)
      assert metadata["format-version"] == 2
      assert metadata["current-snapshot-id"] == -1
      assert metadata["snapshots"] == []
      assert metadata["last-sequence-number"] == 0

      # Verify schema
      [schema] = metadata["schemas"]
      assert schema["type"] == "struct"
      assert length(schema["fields"]) == 5

      # Verify partition spec
      [partition_spec] = metadata["partition-specs"]
      assert partition_spec["spec-id"] == 0
      assert length(partition_spec["fields"]) == 1

      partition_field = List.first(partition_spec["fields"])
      assert partition_field["transform"] == "day"

      # Step 3: Simulate a snapshot (mock parquet stats)
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/canonical/events/data/file1.parquet",
            "file_size_in_bytes" => 2048,
            "record_count" => 500
          }
        ]
      })

      snapshot_opts =
        Keyword.merge(@opts,
          partition_spec: partition_spec,
          sequence_number: 1,
          operation: "overwrite",
          source_file: "sources/events.jsonl"
        )

      {:ok, snapshot} =
        Iceberg.Snapshot.create(
          :conn,
          "canonical/events",
          "memory://test/canonical/events/data/**/*.parquet",
          snapshot_opts
        )

      # Add snapshot to metadata and save
      {:ok, updated_metadata} = Metadata.add_snapshot(metadata, snapshot)
      :ok = Metadata.save("canonical/events", updated_metadata, @opts)

      # Step 4: Verify updated metadata
      {:ok, reloaded} = Metadata.load("canonical/events", @opts)
      assert reloaded["current-snapshot-id"] == snapshot["snapshot-id"]
      assert reloaded["last-sequence-number"] == 1
      assert length(reloaded["snapshots"]) == 1
      assert length(reloaded["snapshot-log"]) == 1
    end

    test "multiple snapshots increment versions correctly" do
      :ok = Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)

      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "memory://test/canonical/simple/data/file1.parquet",
            "file_size_in_bytes" => 1024,
            "record_count" => 100
          }
        ]
      })

      {:ok, initial_metadata} = Metadata.load("canonical/simple", @opts)
      partition_spec = List.first(initial_metadata["partition-specs"])

      # First snapshot
      snapshot_opts =
        Keyword.merge(@opts,
          partition_spec: partition_spec,
          sequence_number: 1,
          operation: "append"
        )

      {:ok, snap1} =
        Iceberg.Snapshot.create(
          :conn,
          "canonical/simple",
          "memory://test/canonical/simple/data/**/*.parquet",
          snapshot_opts
        )

      {:ok, meta1} = Metadata.add_snapshot(initial_metadata, snap1)
      :ok = Metadata.save("canonical/simple", meta1, @opts)

      # Second snapshot
      snapshot_opts2 =
        Keyword.merge(@opts,
          partition_spec: partition_spec,
          sequence_number: 2,
          operation: "append"
        )

      {:ok, snap2} =
        Iceberg.Snapshot.create(
          :conn,
          "canonical/simple",
          "memory://test/canonical/simple/data/**/*.parquet",
          snapshot_opts2
        )

      {:ok, meta2} = Metadata.add_snapshot(meta1, snap2)
      :ok = Metadata.save("canonical/simple", meta2, @opts)

      # Verify
      {:ok, final} = Metadata.load("canonical/simple", @opts)
      assert final["last-sequence-number"] == 2
      assert length(final["snapshots"]) == 2
      assert length(final["snapshot-log"]) == 2
      assert final["current-snapshot-id"] == snap2["snapshot-id"]
    end

    test "version-hint.text tracks latest version" do
      :ok = Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)

      # After create, version-hint should be 0
      {:ok, hint1} = Memory.download("canonical/simple/metadata/version-hint.text")
      assert String.trim(hint1) == "0"

      # After first snapshot
      {:ok, metadata} = Metadata.load("canonical/simple", @opts)
      partition_spec = List.first(metadata["partition-specs"])

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

      snapshot_opts =
        Keyword.merge(@opts, partition_spec: partition_spec, sequence_number: 1)

      {:ok, snapshot} =
        Iceberg.Snapshot.create(:conn, "canonical/simple", "test/**/*.parquet", snapshot_opts)

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)
      :ok = Metadata.save("canonical/simple", updated, @opts)

      {:ok, hint2} = Memory.download("canonical/simple/metadata/version-hint.text")
      assert String.trim(hint2) == "1"
    end

    test "validates complete metadata JSON structure against Iceberg v2 spec" do
      :ok = Table.create(Iceberg.Test.Schemas.Events, nil, @opts)
      {:ok, metadata} = Metadata.load("canonical/events", @opts)

      # All required v2 fields per https://iceberg.apache.org/spec/#table-metadata
      required_fields = [
        "format-version",
        "table-uuid",
        "location",
        "last-sequence-number",
        "last-updated-ms",
        "last-column-id",
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "last-partition-id",
        "properties",
        "current-snapshot-id",
        "snapshots",
        "snapshot-log",
        "metadata-log",
        "sort-orders",
        "default-sort-order-id"
      ]

      for field <- required_fields do
        assert Map.has_key?(metadata, field),
               "Missing required v2 field: #{field}"
      end

      # Type validations
      assert is_integer(metadata["format-version"])
      assert is_binary(metadata["table-uuid"])
      assert is_binary(metadata["location"])
      assert is_integer(metadata["last-sequence-number"])
      assert is_integer(metadata["last-updated-ms"])
      assert is_integer(metadata["last-column-id"])
      assert is_list(metadata["schemas"])
      assert is_integer(metadata["current-schema-id"])
      assert is_list(metadata["partition-specs"])
      assert is_integer(metadata["default-spec-id"])
      assert is_integer(metadata["last-partition-id"])
      assert is_map(metadata["properties"])
      assert is_integer(metadata["current-snapshot-id"])
      assert is_list(metadata["snapshots"])
      assert is_list(metadata["snapshot-log"])
      assert is_list(metadata["metadata-log"])
      assert is_list(metadata["sort-orders"])
      assert is_integer(metadata["default-sort-order-id"])
    end
  end
end
