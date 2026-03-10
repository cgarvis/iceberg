defmodule Iceberg.Maintenance do
  @moduledoc """
  Table maintenance operations for Iceberg tables.

  Provides operations to keep tables healthy and performant by removing
  expired snapshots and their associated orphaned files.
  """

  alias Iceberg.{Metadata, ParquetStats, Snapshot}
  require Logger

  # Default retention policy constants (matching Iceberg spec defaults)
  @default_max_age_ms 432_000_000
  @default_min_snapshots_to_keep 1

  @doc """
  Expires old snapshots from an Iceberg table, deleting orphaned manifest files.

  Snapshots are expired based on age and minimum retention count. The current
  snapshot is never expired regardless of the options provided.

  ## Parameters
    - table_path: Relative path to the table (e.g., "canonical/events")
    - opts: Options
      - `:older_than` - `DateTime.t()` — expire snapshots older than this time.
        Defaults to `now - max_age_ms` (from table properties or default 432_000_000ms).
      - `:retain_last` - Integer — always keep at least this many recent snapshots
        (default: table property "history.expire.min-snapshots-to-keep" or 1).
      - `:snapshot_ids` - List of explicit snapshot IDs to expire.
      - `:dry_run` - Boolean — if true, return counts without deleting (default: false).
      - `:storage` - Storage backend module (required).
      - `:base_url` - Base URL for storage paths (required).

  ## Returns
    `{:ok, %{deleted_data_files: 0, deleted_manifest_files: N, deleted_manifest_lists: M, deleted_snapshots: K}}`
    `{:error, reason}` — operation failed

  ## Notes
    - Data files inside Avro manifests are NOT deleted (Avro decoding is not implemented).
    - `deleted_data_files` is always 0.
  """
  @spec expire_snapshots(String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def expire_snapshots(table_path, opts \\ []) do
    Logger.info(fn -> "Starting expire_snapshots for table: #{table_path}" end)

    with {:ok, metadata} <- Metadata.load(table_path, opts) do
      properties = metadata["properties"] || %{}

      max_age_ms =
        properties
        |> Map.get("history.expire.max-snapshot-age-ms", @default_max_age_ms)
        |> to_integer_safe(@default_max_age_ms)

      min_snapshots_to_keep =
        properties
        |> Map.get("history.expire.min-snapshots-to-keep", @default_min_snapshots_to_keep)
        |> to_integer_safe(@default_min_snapshots_to_keep)

      retain_last = Keyword.get(opts, :retain_last, min_snapshots_to_keep)
      dry_run = Keyword.get(opts, :dry_run, false)

      older_than_ms =
        case Keyword.get(opts, :older_than) do
          nil -> System.system_time(:millisecond) - max_age_ms
          %DateTime{} = dt -> DateTime.to_unix(dt, :millisecond)
        end

      snapshots = metadata["snapshots"] || []
      current_id = metadata["current-snapshot-id"]

      expired_ids =
        determine_expired_ids(snapshots, opts, older_than_ms, retain_last, current_id)

      if MapSet.size(expired_ids) == 0 do
        Logger.debug(fn -> "No snapshots to expire for table: #{table_path}" end)

        {:ok,
         %{
           deleted_data_files: 0,
           deleted_manifest_files: 0,
           deleted_manifest_lists: 0,
           deleted_snapshots: 0
         }}
      else
        perform_expiration(table_path, metadata, snapshots, expired_ids, dry_run, opts)
      end
    end
  end

  @default_target_file_size_bytes 134_217_728

  @doc """
  Compacts small data files in an Iceberg table by merging them into larger files.

  Uses a bin-packing algorithm to group small files together until they reach the
  target file size. Each group is compacted into a single file, then a replace
  snapshot is created to swap the old files for the new ones.

  ## Parameters
    - conn: Compute backend connection
    - table_path: Relative path to the table
    - opts: Options
      - `:target_file_size_bytes` - Target size for output files (default: 134_217_728 = 128MB)
      - `:min_file_size_bytes` - Files smaller than this are candidates for compaction (default: target * 0.75)
      - `:min_input_files` - Minimum number of candidate files to trigger compaction (default: 5)
      - `:dry_run` - If true, return plan without executing (default: false)
      - `:storage`, `:compute`, `:base_url` - Required config opts

  ## Returns
    - `{:ok, %{rewritten_data_files: N, added_data_files: M, rewritten_bytes: B}}` on success
    - `{:ok, %{rewritten_data_files: 0, ...}}` if no compaction needed
    - `{:error, reason}` on failure
  """
  @spec compact_data_files(term(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def compact_data_files(conn, table_path, opts \\ []) do
    Logger.info(fn -> "Starting compact_data_files for table: #{table_path}" end)

    compute = Iceberg.Config.compute_backend(opts)

    unless function_exported?(compute, :compact, 4) do
      raise ArgumentError,
            "Compute backend #{inspect(compute)} does not implement the compact/4 callback"
    end

    target_file_size_bytes =
      Keyword.get(opts, :target_file_size_bytes, @default_target_file_size_bytes)

    min_file_size_bytes =
      Keyword.get(opts, :min_file_size_bytes, round(target_file_size_bytes * 0.75))

    min_input_files = Keyword.get(opts, :min_input_files, 5)
    dry_run = Keyword.get(opts, :dry_run, false)

    with {:ok, metadata} <- Metadata.load(table_path, opts) do
      base_url = Iceberg.Config.base_url(opts)
      data_url_pattern = Iceberg.Config.full_url("#{table_path}/data/**/*.parquet", opts)

      case ParquetStats.extract(conn, data_url_pattern, opts) do
        {:ok, all_stats} ->
          stats_by_path =
            Map.new(all_stats, fn s ->
              relative = strip_base_url(s[:file_path], base_url)
              {relative, s}
            end)

          candidates =
            all_stats
            |> Enum.filter(fn s -> s[:file_size_in_bytes] < min_file_size_bytes end)
            |> Enum.sort_by(& &1[:file_size_in_bytes])
            |> Enum.map(fn s -> strip_base_url(s[:file_path], base_url) end)

          if length(candidates) < min_input_files do
            Logger.debug(fn ->
              "Skipping compaction: #{length(candidates)} candidate files < min_input_files #{min_input_files}"
            end)

            {:ok, %{rewritten_data_files: 0, added_data_files: 0, rewritten_bytes: 0}}
          else
            bins = bin_pack(candidates, stats_by_path, target_file_size_bytes)

            Logger.info(fn ->
              "Compaction plan: #{length(candidates)} files -> #{length(bins)} groups"
            end)

            if dry_run do
              total_rewritten = Enum.sum(Enum.map(bins, &length/1))

              {:ok,
               %{
                 rewritten_data_files: total_rewritten,
                 added_data_files: length(bins),
                 rewritten_bytes:
                   Enum.sum(
                     Enum.map(bins, fn group ->
                       Enum.sum(
                         Enum.map(group, fn path ->
                           stats_by_path[path][:file_size_in_bytes] || 0
                         end)
                       )
                     end)
                   )
               }}
            else
              execute_compaction(
                conn,
                table_path,
                metadata,
                bins,
                stats_by_path,
                compute,
                opts
              )
            end
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  ## Private Functions

  @spec determine_expired_ids(list(map()), keyword(), integer(), integer(), integer() | nil) ::
          MapSet.t()
  defp determine_expired_ids(snapshots, opts, older_than_ms, retain_last, current_id) do
    case Keyword.get(opts, :snapshot_ids) do
      ids when is_list(ids) and length(ids) > 0 ->
        # Explicit IDs: expire those, but never expire current
        ids
        |> MapSet.new()
        |> MapSet.delete(current_id)

      _ ->
        # Age/retain-based expiration
        # Sort by timestamp descending (newest first).
        # Use position (index) as a secondary key so that when two snapshots share the
        # same timestamp-ms, the one added later (higher index) is considered newer.
        sorted =
          snapshots
          |> Enum.with_index()
          |> Enum.sort_by(fn {s, idx} -> {s["timestamp-ms"] || 0, idx} end, :desc)
          |> Enum.map(&elem(&1, 0))

        # Always retain the `retain_last` most recent snapshots
        {_retained_by_count, candidates} = Enum.split(sorted, retain_last)

        # From the remaining candidates, expire those older than the threshold
        candidates
        |> Enum.filter(fn s ->
          ts = s["timestamp-ms"] || 0
          id = s["snapshot-id"]
          ts < older_than_ms && id != current_id
        end)
        |> Enum.map(& &1["snapshot-id"])
        |> MapSet.new()
    end
  end

  @spec perform_expiration(String.t(), map(), list(map()), MapSet.t(), boolean(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defp perform_expiration(table_path, metadata, snapshots, expired_ids, dry_run, opts) do
    storage = Iceberg.Config.storage_backend(opts)
    base_url = Iceberg.Config.base_url(opts)

    # Partition snapshots into retained and expired
    {retained_snapshots, expired_snapshots} =
      Enum.split_with(snapshots, fn s ->
        not MapSet.member?(expired_ids, s["snapshot-id"])
      end)

    # Collect all manifest-list and manifest paths from RETAINED snapshots
    retained_manifest_lists = collect_manifest_lists(retained_snapshots)
    retained_manifests = collect_manifests(retained_snapshots)

    # Collect orphaned manifest-lists (in expired but NOT in retained)
    orphaned_manifest_lists =
      expired_snapshots
      |> collect_manifest_lists()
      |> MapSet.difference(retained_manifest_lists)

    # Collect orphaned manifests (in expired but NOT in retained)
    orphaned_manifests =
      expired_snapshots
      |> collect_manifests()
      |> MapSet.difference(retained_manifests)

    # Use actual expired snapshot count rather than the size of expired_ids.
    # expired_ids may contain IDs that do not exist in the table (e.g. when
    # explicit snapshot_ids are provided), so length(expired_snapshots) is
    # the accurate count of snapshots that will actually be removed.
    deleted_snapshots = length(expired_snapshots)
    deleted_manifest_lists = MapSet.size(orphaned_manifest_lists)
    deleted_manifest_files = MapSet.size(orphaned_manifests)

    Logger.info(fn ->
      "Expiring #{deleted_snapshots} snapshots, #{deleted_manifest_lists} manifest-lists, " <>
        "#{deleted_manifest_files} manifest files (dry_run: #{dry_run})"
    end)

    if dry_run do
      {:ok,
       %{
         deleted_data_files: 0,
         deleted_manifest_files: deleted_manifest_files,
         deleted_manifest_lists: deleted_manifest_lists,
         deleted_snapshots: deleted_snapshots
       }}
    else
      # Delete orphaned files from storage (strip base_url prefix to get relative path)
      Enum.each(orphaned_manifest_lists, fn full_url ->
        relative_path = strip_base_url(full_url, base_url)

        case storage.delete(relative_path, opts) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning(fn ->
              "Failed to delete manifest-list #{relative_path}: #{inspect(reason)}"
            end)
        end
      end)

      Enum.each(orphaned_manifests, fn full_url ->
        relative_path = strip_base_url(full_url, base_url)

        case storage.delete(relative_path, opts) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning(fn ->
              "Failed to delete manifest #{relative_path}: #{inspect(reason)}"
            end)
        end
      end)

      with {:ok, updated_metadata} <- Metadata.remove_snapshots(metadata, expired_ids),
           :ok <- Metadata.save(table_path, updated_metadata, opts) do
        Logger.info(fn ->
          "expire_snapshots complete for #{table_path}: #{deleted_snapshots} snapshots expired"
        end)

        {:ok,
         %{
           deleted_data_files: 0,
           deleted_manifest_files: deleted_manifest_files,
           deleted_manifest_lists: deleted_manifest_lists,
           deleted_snapshots: deleted_snapshots
         }}
      end
    end
  end

  # Collects all manifest-list full URLs from a list of snapshots.
  @spec collect_manifest_lists(list(map())) :: MapSet.t()
  defp collect_manifest_lists(snapshots) do
    snapshots
    |> Enum.map(&get_field(&1, "manifest-list", nil))
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  # Collects all manifest full URLs from snapshots via their manifest-entries.
  @spec collect_manifests(list(map())) :: MapSet.t()
  defp collect_manifests(snapshots) do
    snapshots
    |> Enum.flat_map(fn s ->
      entries = get_field(s, "manifest-entries", [])
      Enum.map(entries, &get_field(&1, :manifest_path, nil))
    end)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  # Retrieves a value from a map using either an atom or string key.
  # Handles the mixed key types that arise from JSON round-trips.
  # Uses explicit nil checks rather than || so that legitimate falsy values
  # (e.g. false, 0) stored under the primary key are returned as-is and do
  # not fall through to the secondary key lookup.
  @spec get_field(map(), atom() | String.t(), term()) :: term()
  defp get_field(map, key, default) when is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, to_string(key), default)
    end
  end

  defp get_field(map, key, default) when is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        atom_key = String.to_existing_atom(key)
        Map.get(map, atom_key, default)
    end
  rescue
    ArgumentError -> default
  end

  # Strips the base_url prefix from a full URL to get the relative storage path.
  @spec strip_base_url(String.t(), String.t()) :: String.t()
  defp strip_base_url(full_url, base_url) do
    base = String.trim_trailing(base_url, "/")
    prefix = base <> "/"

    if String.starts_with?(full_url, prefix) do
      String.replace_prefix(full_url, prefix, "")
    else
      full_url
    end
  end

  @doc """
  Scans the table's metadata directory and deletes files not referenced by any snapshot.

  Orphan files are files that exist in `{table_path}/metadata/` but are not referenced
  by any current snapshot's manifest list or manifest entries. Metadata JSON files
  (matching `v\\d+\\.metadata\\.json`) and `version-hint.text` are never treated as
  orphans — their lifecycle is managed separately.

  Data files under `{table_path}/data/` are **not** scanned because Avro decoding
  is not implemented and referenced data files cannot be determined.

  ## Parameters
    - table_path: Relative path to the table (e.g., "canonical/events")
    - opts: Options
      - `:older_than` - Accepted but not enforced (no file timestamps available).
      - `:dry_run` - Boolean — if true, return candidates without deleting (default: false).
      - `:location` - Override the scan prefix (default: `{table_path}/metadata/`).
      - `:storage` - Storage backend module (required).
      - `:base_url` - Base URL for storage paths (required).

  ## Returns
    - `{:ok, %{deleted_files: N, deleted_paths: [...]}}` — paths are populated only on dry run.
    - `{:error, reason}` — operation failed.
  """
  @spec remove_orphan_files(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def remove_orphan_files(table_path, opts \\ []) do
    Logger.info(fn -> "Starting remove_orphan_files for table: #{table_path}" end)

    with {:ok, metadata} <- Metadata.load(table_path, opts) do
      storage = Iceberg.Config.storage_backend(opts)
      base_url = Iceberg.Config.base_url(opts)
      dry_run = Keyword.get(opts, :dry_run, false)
      prefix = Keyword.get(opts, :location, "#{table_path}/metadata/")

      snapshots = metadata["snapshots"] || []

      # Build the set of referenced relative paths
      referenced =
        build_referenced_set(table_path, snapshots, base_url)

      # List all files under the metadata prefix (returns relative paths)
      all_files = storage.list(prefix, opts)

      # Compute orphans: not referenced AND not a versioned metadata JSON
      metadata_json_pattern = ~r/v\d+\.metadata\.json$/

      orphans =
        Enum.filter(all_files, fn path ->
          not MapSet.member?(referenced, path) and
            not Regex.match?(metadata_json_pattern, path)
        end)

      count = length(orphans)

      Logger.info(fn ->
        "Found #{count} orphan file(s) in #{prefix} (dry_run: #{dry_run})"
      end)

      if dry_run do
        {:ok, %{deleted_files: count, deleted_paths: orphans}}
      else
        Enum.each(orphans, fn path ->
          case storage.delete(path, opts) do
            :ok ->
              :ok

            {:error, reason} ->
              Logger.warning(fn ->
                "Failed to delete orphan #{path}: #{inspect(reason)}"
              end)
          end
        end)

        {:ok, %{deleted_files: count, deleted_paths: []}}
      end
    end
  end

  # Builds a MapSet of relative paths that are referenced by snapshots or are
  # protected system files (version-hint.text).
  @spec build_referenced_set(String.t(), list(map()), String.t()) :: MapSet.t()
  defp build_referenced_set(table_path, snapshots, base_url) do
    manifest_lists =
      snapshots
      |> collect_manifest_lists()
      |> MapSet.new(&strip_base_url(&1, base_url))

    manifests =
      snapshots
      |> collect_manifests()
      |> MapSet.new(&strip_base_url(&1, base_url))

    protected =
      MapSet.new(["#{table_path}/metadata/version-hint.text"])

    protected
    |> MapSet.union(manifest_lists)
    |> MapSet.union(manifests)
  end

  @spec execute_compaction(term(), String.t(), map(), list(list(String.t())), map(), module(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defp execute_compaction(conn, table_path, metadata, bins, stats_by_path, compute, opts) do
    results =
      Enum.map(bins, fn group ->
        output_uuid = Iceberg.UUID.generate()
        output_relative = "#{table_path}/data/#{output_uuid}.parquet"
        output_path = Iceberg.Config.full_url(output_relative, opts)

        input_full_paths =
          Enum.map(group, &Iceberg.Config.full_url(&1, opts))

        case compute.compact(conn, input_full_paths, output_path, opts) do
          {:ok, _result} ->
            {:ok, %{group: group, output_relative: output_relative}}

          {:error, reason} ->
            Logger.error(fn ->
              "Compaction failed for group #{inspect(group)}: #{inspect(reason)}"
            end)

            {:error, reason}
        end
      end)

    {compacted_groups, errors} = Enum.split_with(results, &match?({:ok, _}, &1))

    if errors != [] do
      {:error, {:compaction_failed, Enum.map(errors, &elem(&1, 1))}}
    else
      successful = Enum.map(compacted_groups, &elem(&1, 1))

      deleted_files_metadata =
        Enum.flat_map(successful, fn %{group: group} ->
          Enum.map(group, fn path ->
            stat = stats_by_path[path]

            %{
              file_path: Iceberg.Config.full_url(path, opts),
              file_size_in_bytes: stat[:file_size_in_bytes] || 0,
              record_count: stat[:record_count] || 0,
              partition_values: stat[:partition_values] || %{}
            }
          end)
        end)

      new_file_paths = Enum.map(successful, & &1[:output_relative])
      new_data_url_pattern = build_compaction_pattern(new_file_paths, table_path, opts)

      partition_spec =
        List.first(metadata["partition-specs"]) ||
          %{"spec-id" => 0, "fields" => []}

      sequence_number = (metadata["last-sequence-number"] || 0) + 1
      table_schema = List.first(metadata["schemas"])
      schema_id = metadata["current-schema-id"] || 0
      parent_manifests = get_current_snapshot_manifests(metadata)

      snapshot_opts =
        Keyword.merge(opts,
          partition_spec: partition_spec,
          sequence_number: sequence_number,
          table_schema: table_schema,
          schema_id: schema_id,
          parent_manifests: parent_manifests
        )

      with {:ok, snapshot} <-
             Snapshot.create_replace(
               conn,
               table_path,
               deleted_files_metadata,
               new_data_url_pattern,
               snapshot_opts
             ),
           {:ok, new_metadata} <- Metadata.add_snapshot(metadata, snapshot),
           :ok <- Metadata.save(table_path, new_metadata, opts) do
        total_rewritten = length(deleted_files_metadata)
        total_added = length(successful)

        rewritten_bytes =
          Enum.sum(Enum.map(deleted_files_metadata, & &1[:file_size_in_bytes]))

        Logger.info(fn ->
          "compact_data_files complete for #{table_path}: #{total_rewritten} files -> #{total_added} files"
        end)

        {:ok,
         %{
           rewritten_data_files: total_rewritten,
           added_data_files: total_added,
           rewritten_bytes: rewritten_bytes
         }}
      end
    end
  end

  # Builds a file pattern for ParquetStats.extract that matches only the compacted output files.
  # Uses the exact single path when there's one file, or constructs a brace glob for multiple.
  @spec build_compaction_pattern(list(String.t()), String.t(), keyword()) :: String.t()
  defp build_compaction_pattern([single], _table_path, opts) do
    Iceberg.Config.full_url(single, opts)
  end

  defp build_compaction_pattern(paths, _table_path, opts) do
    filenames = Enum.map(paths, &Path.basename/1)
    dir = paths |> List.first() |> Path.dirname()
    pattern = "#{dir}/{#{Enum.join(filenames, ",")}}"
    Iceberg.Config.full_url(pattern, opts)
  end

  # Groups candidate files into bins using a first-fit bin-packing algorithm.
  # Each bin accumulates files until adding the next would exceed target_file_size_bytes.
  # Bins with fewer than 2 files are discarded (single files need no compaction).
  @spec bin_pack(list(String.t()), map(), integer()) :: list(list(String.t()))
  defp bin_pack(candidates, stats_by_path, target_file_size_bytes) do
    {rev_bins, current_bin, _current_size} =
      Enum.reduce(candidates, {[], [], 0}, fn path, {bins, current_bin, current_size} ->
        file_size = stats_by_path[path][:file_size_in_bytes] || 0

        if current_bin != [] && current_size + file_size > target_file_size_bytes do
          {[Enum.reverse(current_bin) | bins], [path], file_size}
        else
          {bins, [path | current_bin], current_size + file_size}
        end
      end)

    final_bins =
      if current_bin != [] do
        [Enum.reverse(current_bin) | rev_bins]
      else
        rev_bins
      end
      |> Enum.reverse()

    Enum.filter(final_bins, fn bin -> length(bin) >= 2 end)
  end

  # Extracts accumulated manifest entries from the current snapshot's JSON metadata.
  @spec get_current_snapshot_manifests(map()) :: list(map())
  defp get_current_snapshot_manifests(metadata) do
    current_id = metadata["current-snapshot-id"]

    if current_id && current_id > 0 do
      metadata["snapshots"]
      |> List.wrap()
      |> Enum.find(&(&1["snapshot-id"] == current_id))
      |> case do
        nil -> []
        snapshot -> snapshot["manifest-entries"] || []
      end
    else
      []
    end
  end

  @spec to_integer_safe(integer() | String.t() | term(), integer()) :: integer()
  defp to_integer_safe(value, _default) when is_integer(value), do: value

  defp to_integer_safe(value, _default) when is_binary(value) do
    String.to_integer(value)
  end

  defp to_integer_safe(_value, default), do: default
end
