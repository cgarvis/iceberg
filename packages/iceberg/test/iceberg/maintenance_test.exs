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

  describe "remove_orphan_files/2" do
    test "no orphan files returns zero counts" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)
      create_snapshot("test/table", "file1.parquet")

      assert {:ok, result} = Table.remove_orphan_files("test/table", @opts)

      assert result.deleted_files == 0
      assert result.deleted_paths == []
    end

    test "detects and deletes orphaned avro files" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)
      create_snapshot("test/table", "file1.parquet")

      # Upload a fake orphan file into the metadata directory
      Memory.upload("test/table/metadata/orphan-file.avro", "fake", [])

      assert {:ok, result} = Table.remove_orphan_files("test/table", @opts)

      assert result.deleted_files == 1

      # The orphan should be gone from storage
      files = Memory.dump()
      refute Map.has_key?(files, "test/table/metadata/orphan-file.avro")
    end

    test "does not delete referenced manifest files" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)
      create_snapshot("test/table", "file1.parquet")

      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_id = metadata["current-snapshot-id"]
      current_snap = Enum.find(metadata["snapshots"], &(&1["snapshot-id"] == current_id))

      manifest_paths =
        (current_snap["manifest-entries"] || [])
        |> Enum.map(&(&1["manifest_path"] || &1[:manifest_path]))
        |> Enum.reject(&is_nil/1)

      manifest_list_path = current_snap["manifest-list"]

      assert {:ok, _result} = Table.remove_orphan_files("test/table", @opts)

      files_after = Memory.dump()

      # All manifest files referenced by the snapshot must still exist
      for path <- manifest_paths do
        relative = String.replace_prefix(path, "memory://test/", "")

        assert Map.has_key?(files_after, relative),
               "Expected referenced manifest to still exist: #{relative}"
      end

      # The manifest list itself must still exist
      if manifest_list_path do
        relative_ml = String.replace_prefix(manifest_list_path, "memory://test/", "")

        assert Map.has_key?(files_after, relative_ml),
               "Expected manifest list to still exist: #{relative_ml}"
      end
    end

    test "dry_run returns candidates without deleting" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)
      create_snapshot("test/table", "file1.parquet")

      Memory.upload("test/table/metadata/orphan-file.avro", "fake", [])
      files_before = Memory.dump()

      assert {:ok, result} =
               Table.remove_orphan_files("test/table", Keyword.put(@opts, :dry_run, true))

      assert result.deleted_files == 1
      assert "test/table/metadata/orphan-file.avro" in result.deleted_paths

      # File must still exist after dry run
      files_after = Memory.dump()
      assert map_size(files_before) == map_size(files_after)
      assert Map.has_key?(files_after, "test/table/metadata/orphan-file.avro")
    end

    test "does not delete version-hint.text or metadata JSON files" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:ok, result} = Table.remove_orphan_files("test/table", @opts)

      assert result.deleted_files == 0

      files = Memory.dump()
      assert Map.has_key?(files, "test/table/metadata/version-hint.text")

      # At least one versioned metadata JSON should still be present
      metadata_jsons =
        files
        |> Map.keys()
        |> Enum.filter(&Regex.match?(~r/v\d+\.metadata\.json$/, &1))

      assert length(metadata_jsons) > 0
    end

    test "handles table with no snapshots" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      Memory.upload("test/table/metadata/orphan-file.avro", "fake", [])

      assert {:ok, result} = Table.remove_orphan_files("test/table", @opts)

      assert result.deleted_files == 1

      files = Memory.dump()
      refute Map.has_key?(files, "test/table/metadata/orphan-file.avro")
    end

    test "cleans up files left behind by expire_snapshots" do
      # Validates the primary collaboration between the two maintenance operations:
      # expire_snapshots removes snapshot entries from metadata, and
      # remove_orphan_files is responsible for cleaning up the actual storage files
      # that belonged to those expired snapshots (manifest lists whose snapshots no
      # longer exist in the metadata log).
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")

      # Record all .avro files present before expiration
      files_before = Memory.dump()

      avro_keys_before =
        files_before |> Map.keys() |> Enum.filter(&String.ends_with?(&1, ".avro")) |> MapSet.new()

      # Expire the first snapshot (retain_last: 1 keeps only the current one)
      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      assert {:ok, expire_result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 1)
               )

      assert expire_result.deleted_manifest_lists == 1

      # After expire_snapshots the metadata no longer references the old manifest list,
      # but the snap-*.avro file for the expired snapshot may still be in storage
      # if expire_snapshots deleted the manifest-list file it should be gone, but
      # a manifest (.avro) that was shared via parent_manifests may still be around.
      # remove_orphan_files must delete any remaining unreferenced .avro files.
      assert {:ok, orphan_result} = Table.remove_orphan_files("test/table", @opts)

      files_after = Memory.dump()

      avro_keys_after =
        files_after |> Map.keys() |> Enum.filter(&String.ends_with?(&1, ".avro")) |> MapSet.new()

      # The combined cleanup must have reduced the avro file count
      assert MapSet.size(avro_keys_after) < MapSet.size(avro_keys_before),
             "Expected fewer .avro files after both maintenance operations"

      # Confirm the deleted count is consistent
      assert expire_result.deleted_manifest_lists + orphan_result.deleted_files ==
               MapSet.size(avro_keys_before) - MapSet.size(avro_keys_after)
    end

    test "does not delete manifest list files referenced by multiple snapshots" do
      # A manifest-list file belongs to exactly one snapshot, but a manifest (content
      # file) can be shared across snapshots via parent_manifests carry-forward.
      # Verify that remove_orphan_files does not delete shared manifests.
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")

      # Both snapshots reference file1's manifest via parent_manifests carry-forward
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_id = metadata["current-snapshot-id"]
      current_snap = Enum.find(metadata["snapshots"], &(&1["snapshot-id"] == current_id))

      shared_manifest_paths =
        (current_snap["manifest-entries"] || [])
        |> Enum.map(&(&1["manifest_path"] || &1[:manifest_path]))
        |> Enum.reject(&is_nil/1)
        |> Enum.map(&String.replace_prefix(&1, "memory://test/", ""))

      assert length(shared_manifest_paths) >= 1

      assert {:ok, result} = Table.remove_orphan_files("test/table", @opts)
      assert result.deleted_files == 0

      files_after = Memory.dump()

      for path <- shared_manifest_paths do
        assert Map.has_key?(files_after, path),
               "Shared manifest #{path} must not be deleted by remove_orphan_files"
      end
    end
  end

  describe "rewrite_manifests/2" do
    test "no snapshots returns zero counts" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:ok, result} = Table.rewrite_manifests("test/table", @opts)

      assert result == %{rewritten_manifests: 0, added_manifests: 0}
    end

    test "single manifest entry returns zero counts" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")

      assert {:ok, result} = Table.rewrite_manifests("test/table", @opts)

      assert result == %{rewritten_manifests: 0, added_manifests: 0}
    end

    test "rewrites manifest list with multiple entries" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")
      create_snapshot("test/table", "file3.parquet")

      # Verify current snapshot has 3 manifest entries (via parent_manifests carry-forward)
      {:ok, metadata_before} = Iceberg.Metadata.load("test/table", @opts)
      current_id_before = metadata_before["current-snapshot-id"]

      current_snap_before =
        Enum.find(metadata_before["snapshots"], &(&1["snapshot-id"] == current_id_before))

      assert length(current_snap_before["manifest-entries"]) == 3

      assert {:ok, result} = Table.rewrite_manifests("test/table", @opts)

      assert result == %{rewritten_manifests: 3, added_manifests: 1}

      # Verify a new snapshot was created
      {:ok, metadata_after} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata_after["snapshots"]) == 4

      # Verify the new snapshot has 3 manifest entries
      new_current_id = metadata_after["current-snapshot-id"]
      new_current_snap = Enum.find(metadata_after["snapshots"], &(&1["snapshot-id"] == new_current_id))
      assert length(new_current_snap["manifest-entries"]) == 3
    end

    test "dry_run returns counts without modifying" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")
      create_snapshot("test/table", "file3.parquet")

      assert {:ok, result} =
               Table.rewrite_manifests("test/table", Keyword.put(@opts, :dry_run, true))

      assert result == %{rewritten_manifests: 3, added_manifests: 1}

      # Metadata should be unchanged (still 3 snapshots)
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) == 3
    end

    test "works after expire_snapshots" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      create_snapshot("test/table", "file1.parquet")
      create_snapshot("test/table", "file2.parquet")
      create_snapshot("test/table", "file3.parquet")

      older_than = DateTime.utc_now() |> DateTime.add(1, :second)

      assert {:ok, expire_result} =
               Table.expire_snapshots(
                 "test/table",
                 Keyword.merge(@opts, older_than: older_than, retain_last: 1)
               )

      assert expire_result.deleted_snapshots == 2

      # Now rewrite manifests on the post-expiration table
      assert {:ok, rewrite_result} = Table.rewrite_manifests("test/table", @opts)

      # The surviving snapshot should have carried-forward manifest entries
      # If it has more than 1, rewrite should consolidate; if 1, returns zero counts.
      # Either way, the call should succeed.
      assert rewrite_result.added_manifests in [0, 1]

      # Verify metadata is consistent
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_id = metadata["current-snapshot-id"]
      assert current_id != nil
      current_snap = Enum.find(metadata["snapshots"], &(&1["snapshot-id"] == current_id))
      assert current_snap != nil
    end
  end

  describe "compact_data_files/3" do
    # Helper to seed small files without creating a snapshot so we can control
    # how many candidate files exist independently of snapshot creation.
    defp seed_small_file(table_path, filename, size_bytes \\ 1024) do
      relative_path = "#{table_path}/data/#{filename}"
      full_path = "memory://test/#{relative_path}"
      Memory.upload(relative_path, "fake-parquet-data-#{filename}", [])
      {relative_path, full_path, size_bytes}
    end

    # Sets up MockCompute to respond to parquet_metadata queries with stats for
    # the given list of {relative_path, full_path, size_bytes} tuples.
    defp set_stats_response(file_tuples) do
      rows =
        Enum.map(file_tuples, fn {_rel, full, size} ->
          %{
            "file_path" => full,
            "file_size_in_bytes" => size,
            "record_count" => 10
          }
        end)

      MockCompute.set_response("parquet_metadata", {:ok, rows})
    end

    test "returns zero counts when no files exist" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      MockCompute.set_response("parquet_metadata", {:ok, []})

      assert {:ok, result} = Table.compact_data_files(:conn, "test/table", @opts)

      assert result == %{rewritten_data_files: 0, added_data_files: 0, rewritten_bytes: 0}
    end

    test "returns zero counts when fewer than min_input_files candidates" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      files = [
        seed_small_file("test/table", "small1.parquet", 512),
        seed_small_file("test/table", "small2.parquet", 512)
      ]

      set_stats_response(files)

      opts = Keyword.put(@opts, :min_input_files, 5)
      assert {:ok, result} = Table.compact_data_files(:conn, "test/table", opts)

      assert result == %{rewritten_data_files: 0, added_data_files: 0, rewritten_bytes: 0}
    end

    test "compacts small files into larger ones" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      files = [
        seed_small_file("test/table", "small1.parquet", 512),
        seed_small_file("test/table", "small2.parquet", 512),
        seed_small_file("test/table", "small3.parquet", 512),
        seed_small_file("test/table", "small4.parquet", 512),
        seed_small_file("test/table", "small5.parquet", 512)
      ]

      # Set the parquet_metadata response. This is used twice by compact_data_files:
      # once for candidate selection, and once by Snapshot.create_replace for the
      # new file manifest. The same response is acceptable for both calls.
      set_stats_response(files)

      opts =
        Keyword.merge(@opts,
          min_input_files: 5,
          target_file_size_bytes: 134_217_728,
          min_file_size_bytes: 100_000
        )

      assert {:ok, result} = Table.compact_data_files(:conn, "test/table", opts)

      assert result.rewritten_data_files == 5
      assert result.added_data_files == 1
      assert result.rewritten_bytes == 5 * 512

      # Verify a replace snapshot was created
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert length(metadata["snapshots"]) >= 1
      current_id = metadata["current-snapshot-id"]
      current_snap = Enum.find(metadata["snapshots"], &(&1["snapshot-id"] == current_id))
      assert current_snap["summary"]["operation"] == "replace"
    end

    test "dry_run returns plan without executing" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      files = [
        seed_small_file("test/table", "small1.parquet", 512),
        seed_small_file("test/table", "small2.parquet", 512),
        seed_small_file("test/table", "small3.parquet", 512),
        seed_small_file("test/table", "small4.parquet", 512),
        seed_small_file("test/table", "small5.parquet", 512)
      ]

      set_stats_response(files)

      opts =
        Keyword.merge(@opts,
          min_input_files: 5,
          target_file_size_bytes: 134_217_728,
          min_file_size_bytes: 100_000,
          dry_run: true
        )

      assert {:ok, result} = Table.compact_data_files(:conn, "test/table", opts)

      assert result.rewritten_data_files == 5
      assert result.added_data_files == 1
      assert result.rewritten_bytes == 5 * 512

      # No snapshot should be created since it's a dry run
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert metadata["snapshots"] == nil or metadata["snapshots"] == []
    end

    test "skips files above min_file_size_bytes threshold" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      small_files = [
        seed_small_file("test/table", "small1.parquet", 512),
        seed_small_file("test/table", "small2.parquet", 512),
        seed_small_file("test/table", "small3.parquet", 512),
        seed_small_file("test/table", "small4.parquet", 512),
        seed_small_file("test/table", "small5.parquet", 512)
      ]

      large_files = [
        seed_small_file("test/table", "large1.parquet", 200_000_000),
        seed_small_file("test/table", "large2.parquet", 200_000_000)
      ]

      # Set response for both the candidate selection query and the post-compact
      # Snapshot.create_replace query. The same mock is acceptable for both.
      set_stats_response(small_files ++ large_files)

      opts =
        Keyword.merge(@opts,
          min_input_files: 5,
          target_file_size_bytes: 134_217_728,
          min_file_size_bytes: 100_000
        )

      assert {:ok, result} = Table.compact_data_files(:conn, "test/table", opts)

      # Only the 5 small files should be compacted; 2 large files are skipped
      assert result.rewritten_data_files == 5
      assert result.added_data_files == 1
    end
  end
end
