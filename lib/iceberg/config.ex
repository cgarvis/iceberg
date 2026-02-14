defmodule Iceberg.Config do
  @moduledoc """
  Configuration system for Iceberg.

  All configuration must be passed explicitly via opts. There is no
  application config fallback - this library is environment-agnostic.

  ## Required Configuration

  When calling Iceberg functions, provide these opts:

      Iceberg.Table.create(schema, nil,
        storage: MyApp.S3Storage,
        compute: MyApp.DuckDBCompute,
        base_url: "s3://my-bucket")
  """

  @doc """
  Gets the storage backend module.

  ## Examples

      iex> Iceberg.Config.storage_backend(storage: MyApp.CustomStorage)
      MyApp.CustomStorage
  """
  @spec storage_backend(keyword()) :: module()
  def storage_backend(opts) do
    opts[:storage] || raise_missing(:storage)
  end

  @doc """
  Gets the compute backend module.

  ## Examples

      iex> Iceberg.Config.compute_backend(compute: MyApp.CustomCompute)
      MyApp.CustomCompute
  """
  @spec compute_backend(keyword()) :: module()
  def compute_backend(opts) do
    opts[:compute] || raise_missing(:compute)
  end

  @doc """
  Gets the base URL for storage paths.

  This is the root URL where all table data is stored.

  ## Examples

      iex> Iceberg.Config.base_url(base_url: "s3://other-bucket")
      "s3://other-bucket"

  ## Supported URL Schemes

  - S3: `s3://bucket-name`
  - GCS: `gs://bucket-name`
  - Local: `file:///path/to/data`
  - Azure: `wasbs://container@account.blob.core.windows.net`
  """
  @spec base_url(keyword()) :: String.t()
  def base_url(opts) do
    opts[:base_url] || raise_missing(:base_url)
  end

  @doc """
  Builds a full storage URL from base URL and relative path.

  ## Examples

      iex> Iceberg.Config.full_url("data/file.parquet", base_url: "s3://other")
      "s3://other/data/file.parquet"
  """
  @spec full_url(String.t(), keyword()) :: String.t()
  def full_url(path, opts) do
    base = base_url(opts)
    # Remove trailing slash from base, leading slash from path
    base = String.trim_trailing(base, "/")
    path = String.trim_leading(path, "/")
    "#{base}/#{path}"
  end

  @doc """
  Gets a configuration value with fallback.

  ## Examples

      iex> Iceberg.Config.get(:my_setting, [my_setting: "value"], "default")
      "value"

      iex> Iceberg.Config.get(:my_setting, [], "default")
      "default"
  """
  @spec get(atom(), keyword(), term()) :: term()
  def get(key, opts, default) do
    Keyword.get(opts, key, default)
  end

  ## Private Functions

  defp raise_missing(key) do
    raise ArgumentError, """
    Iceberg configuration missing: #{key}

    Pass configuration via opts:

        Iceberg.Table.create(schema, nil,
          storage: MyApp.Storage,
          compute: MyApp.Compute,
          base_url: "s3://bucket")
    """
  end
end
