defmodule Iceberg.Storage do
  @moduledoc """
  Behaviour for storage backends (S3, GCS, local filesystem, etc.)

  Defines the interface that all storage backends must implement for
  reading and writing Iceberg table files (metadata, manifests, data).

  ## Example Implementation

      defmodule MyApp.S3Storage do
        @behaviour Iceberg.Storage

        @impl true
        def upload(path, content, opts) do
          # Upload to S3
          :ok
        end

        @impl true
        def download(path) do
          # Download from S3
          {:ok, binary}
        end

        # ... implement other callbacks
      end
  """

  @type path :: String.t()
  @type content :: binary()
  @type opts :: keyword()
  @type error :: term()

  @doc """
  Uploads content to the storage backend.

  ## Parameters
    - path: Relative path from base URL (e.g., "canonical/events/metadata/v1.metadata.json")
    - content: Binary content to upload
    - opts: Options including :content_type

  ## Returns
    - `:ok` on success
    - `{:error, reason}` on failure
  """
  @callback upload(path, content, opts) :: :ok | {:error, error}

  @doc """
  Downloads content from the storage backend.

  ## Parameters
    - path: Relative path from base URL

  ## Returns
    - `{:ok, binary}` on success
    - `{:error, :not_found}` if file doesn't exist
    - `{:error, reason}` on other failures
  """
  @callback download(path) :: {:ok, content} | {:error, error}

  @doc """
  Lists all objects with the given prefix.

  ## Parameters
    - prefix: Path prefix to list (e.g., "canonical/events/metadata/")

  ## Returns
    - List of relative paths
    - `{:error, reason}` on failure
  """
  @callback list(prefix :: path) :: list(path) | {:error, error}

  @doc """
  Deletes an object from storage.

  ## Parameters
    - path: Relative path to delete

  ## Returns
    - `:ok` on success (even if file doesn't exist)
    - `{:error, reason}` on failure
  """
  @callback delete(path) :: :ok | {:error, error}

  @doc """
  Checks if an object exists.

  ## Parameters
    - path: Relative path to check

  ## Returns
    - `true` if exists
    - `false` if doesn't exist
  """
  @callback exists?(path) :: boolean()
end
