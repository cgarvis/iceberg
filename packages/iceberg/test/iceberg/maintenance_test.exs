defmodule Iceberg.MaintenanceTest do
  use ExUnit.Case

  alias Iceberg.Storage.Memory
  alias Iceberg.Table
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

  # Helper to seed a parquet file in memory storage and configure MockCompute
  # to report its stats, then call Table.register_files to create a snapshot.
  # Uses an explicit snapshot_id (counter-based) to avoid millisecond collisions.
  defp create_snapshot(table_path, filename, opts \\ @opts) do
    relative_path = "#{table_path}/data/#{filename}"
    full_path = "memory://test/#{relative_path}"

    Memory.upload(relative_path, "fake-parquet-data-#{filename}", [])

    MockCompute.set_response("parquet_metadata", {
      :ok,
      [
        %{
          "file_path" => full_path,
          "file_size_in_bytes" => 1024,
          "record_count" => 100
        }
      ]
    })

    # Use a unique snapshot_id based on monotonic time with nanosecond resolution
    # to guarantee uniqueness even within the same millisecond
    snapshot_id = System.unique_integer([:positive, :monotonic])

    snapshot_opts = Keyword.put(opts, :snapshot_id, snapshot_id)

    {:ok, snapshot} =
      Table.register_files(:conn, table_path, "#{table_path}/data/**/*.parquet", snapshot_opts)

    snapshot
  end

  describe "expire_snapshots/2" do
    test "no snapshots returns zero counts" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:ok, result} = Table.expire_snapshots("test/table", @opts)

      assert result == %{
               deleted_data_files: 0,
               deleted_manifest_files: 0,
               deleted_manifest_lists: 0,
               deleted_snapshots: 0
             }
    end

    test "retains current snapshot even when older_than is in the future" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")

      # older_than is 1 second in the future — all snapshots qualify for expiry by age
      # but the current snapshot must always be retained
      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 0)
               )

      # Current snapshot should NOT be deleted even with retain_last: 0
      assert result.deleted_snapshots == 0
      assert result.deleted_manifest_files == 0
      assert result.deleted_manifest_lists == 0
    end

    test "expires old snapshots beyond retain_last" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      # Create 3 snapshots with guaranteed unique IDs
      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")
      create_snapshot("test/table", "file3.parquet")

      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 1)
               )

      # retain_last: 1 keeps only the most recent (which is also current);
      # the other 2 should be expired
      assert result.deleted_snapshots == 2

      # Verify metadata was updated
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) == 1
    end

    test "respects retain_last — keeps specified number of recent snapshots" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")
      create_snapshot("test/table", "file3.parquet")

      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 2)
               )

      # retain_last: 2 keeps the 2 most recent; the oldest 1 is expired
      assert result.deleted_snapshots == 1

      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) == 2
    end

    test "deletes orphaned manifest files from storage" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")

      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      # Before expiration: should have .avro files in metadata
      files_before = Memory.dump()

      avro_files_before =
        files_before |> Map.keys() |> Enum.filter(&String.ends_with?(&1, ".avro"))

      assert length(avro_files_before) > 0

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 1)
               )

      assert result.deleted_manifest_files > 0 or result.deleted_manifest_lists > 0

      # After expiration: orphaned .avro files should be deleted
      files_after = Memory.dump()
      avro_files_after = files_after |> Map.keys() |> Enum.filter(&String.ends_with?(&1, ".avro"))

      # There should be fewer .avro files after expiration
      assert length(avro_files_after) < length(avro_files_before)
    end

    test "does not delete shared manifest files referenced by retained snapshots" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      # Create first snapshot — its manifests are carried forward into the second snapshot
      # via parent_manifests (append operation). The manifest from snapshot 1 is thus
      # referenced by both snapshot 1 AND snapshot 2's manifest-entries.
      _snap1 = create_snapshot("test/table", "file1.parquet")
      _snap2 = create_snapshot("test/table", "file2.parquet")

      # Get manifest paths from the retained (current/2nd) snapshot BEFORE expiration.
      # After Metadata.load, manifest-entries have string keys due to JSON round-trip.
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_id = metadata["current-snapshot-id"]
      current_snap = Enum.find(metadata["snapshots"], &(&1["snapshot-id"] == current_id))

      retained_manifest_paths =
        (current_snap["manifest-entries"] || [])
        |> Enum.map(&(&1["manifest_path"] || &1[:manifest_path]))
        |> Enum.reject(&is_nil/1)

      assert length(retained_manifest_paths) > 0,
             "Expected current snapshot to have manifest entries"

      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      assert {:ok, _result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 1)
               )

      # Manifests referenced by the retained (current) snapshot must still exist
      files_after = Memory.dump()

      for path <- retained_manifest_paths do
        # Strip the base_url prefix to get the storage key
        relative = String.replace_prefix(path, "memory://test/", "")

        assert Map.has_key?(files_after, relative),
               "Expected retained manifest to still exist: #{relative}"
      end
    end

    test "dry_run returns counts without deleting any files" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")

      older_than = DateTime.utc_now() |> DateTime.add(1, :second)
      files_before = Memory.dump()

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 1, dry_run: true)
               )

      # dry_run should report non-zero deletions
      assert result.deleted_snapshots > 0

      # But no files should have been deleted
      files_after = Memory.dump()
      assert map_size(files_before) == map_size(files_after)

      # Metadata should be unchanged (both snapshots still present)
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) == 2
    end

    test "with explicit snapshot_ids expires only the specified snapshot" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      snap1 = create_snapshot("test/table", "file1.parquet")
      snap2 = create_snapshot("test/table", "file2.parquet")
      _snap3 = create_snapshot("test/table", "file3.parquet")

      # Expire only snapshot 1 (the oldest, not the current)
      snap1_id = snap1["snapshot-id"]
      snap2_id = snap2["snapshot-id"]

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, snapshot_ids: [snap1_id])
               )

      assert result.deleted_snapshots == 1

      # Snapshot 1 should be gone; snapshots 2 and 3 should remain
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      remaining_ids = Enum.map(metadata["snapshots"], & &1["snapshot-id"])

      refute snap1_id in remaining_ids
      assert snap2_id in remaining_ids
    end

    test "with explicit snapshot_ids that do not exist returns zero deleted" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")

      # IDs that do not correspond to any real snapshot
      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, snapshot_ids: [99_999_999])
               )

      # No real snapshot was removed; count must reflect actual deletions
      assert result.deleted_snapshots == 0

      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) == 1
    end

    test "with explicit snapshot_ids containing only the current snapshot returns zero deleted" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      snap = create_snapshot("test/table", "file1.parquet")
      current_id = snap["snapshot-id"]

      assert {:ok, result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, snapshot_ids: [current_id])
               )

      # Current snapshot must never be expired
      assert result.deleted_snapshots == 0

      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) == 1
    end
  end
end
