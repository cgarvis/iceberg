defmodule Iceberg.Maintenance do
  @moduledoc """
  Table maintenance operations for Iceberg tables.

  Provides operations to keep tables healthy and performant by removing
  expired snapshots and their associated orphaned files.
  """

  alias Iceberg.Metadata
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
  @spec get_field(map(), atom() | String.t(), term()) :: term()
  defp get_field(map, key, default) when is_atom(key) do
    map[key] || map[to_string(key)] || default
  end

  defp get_field(map, key, default) when is_binary(key) do
    map[key] || map[String.to_existing_atom(key)] || default
  rescue
    ArgumentError -> map[key] || default
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

  @spec to_integer_safe(integer() | String.t() | term(), integer()) :: integer()
  defp to_integer_safe(value, _default) when is_integer(value), do: value

  defp to_integer_safe(value, _default) when is_binary(value) do
    String.to_integer(value)
  end

  defp to_integer_safe(_value, default), do: default
end
