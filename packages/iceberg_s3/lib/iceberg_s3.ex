defmodule IcebergS3 do
  @moduledoc """
  S3 storage adapter for Apache Iceberg.

  This module implements the `Iceberg.Storage` behavior using AWS S3 as the
  storage backend via ExAws.

  ## Usage

      # Configure S3 credentials (optional - will use ExAws default chain if not provided)
      config :iceberg_s3,
        access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
        region: "us-east-1"

      # Use in Iceberg operations
      opts = [
        storage: IcebergS3,
        compute: IcebergDuckdb,
        base_url: "s3://my-bucket/path/prefix"
      ]

      Iceberg.Table.create(schema, nil, opts)

  ## S3-Compatible Services

  To use with S3-compatible services like MinIO or Cloudflare R2:

      config :iceberg_s3,
        endpoint: "http://localhost:9000",  # MinIO
        region: "auto"                       # Some services use "auto"

  ## Base URL Format

  The `base_url` should be in the format:
  - `s3://bucket-name` - Store at bucket root
  - `s3://bucket-name/path/prefix` - Store under a prefix

  All relative paths passed to storage operations will be appended to this base URL.
  """

  @behaviour Iceberg.Storage

  require Logger

  alias IcebergS3.Config

  @impl true
  def upload(path, content, opts) do
    {bucket, key} = parse_s3_path(path, opts)
    content_type = Keyword.get(opts, :content_type, "application/octet-stream")

    Logger.debug("S3 upload: s3://#{bucket}/#{key}")

    ExAws.S3.put_object(bucket, key, content, content_type: content_type)
    |> ExAws.request(Config.ex_aws_config())
    |> case do
      {:ok, _response} ->
        :ok

      {:error, {:http_error, 404, _}} ->
        {:error, :bucket_not_found}

      {:error, reason} ->
        Logger.error("S3 upload failed for s3://#{bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def download(path, opts) do
    {bucket, key} = parse_s3_path(path, opts)

    Logger.debug("S3 download: s3://#{bucket}/#{key}")

    ExAws.S3.get_object(bucket, key)
    |> ExAws.request(Config.ex_aws_config())
    |> case do
      {:ok, %{body: body}} ->
        {:ok, body}

      {:error, {:http_error, 404, _}} ->
        {:error, :not_found}

      {:error, reason} ->
        Logger.error("S3 download failed for s3://#{bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def list(prefix, opts) do
    {bucket, key_prefix} = parse_s3_path(prefix, opts)

    Logger.debug("S3 list: s3://#{bucket}/#{key_prefix}")

    try do
      ExAws.S3.list_objects_v2(bucket, prefix: key_prefix)
      |> ExAws.stream!(Config.ex_aws_config())
      |> Stream.map(& &1.key)
      |> Enum.to_list()
    rescue
      error ->
        Logger.error("S3 list failed for s3://#{bucket}/#{key_prefix}: #{inspect(error)}")
        {:error, error}
    end
  end

  @impl true
  def delete(path, opts) do
    {bucket, key} = parse_s3_path(path, opts)

    Logger.debug("S3 delete: s3://#{bucket}/#{key}")

    ExAws.S3.delete_object(bucket, key)
    |> ExAws.request(Config.ex_aws_config())
    |> case do
      {:ok, _response} ->
        :ok

      # S3 returns success even if the object doesn't exist
      {:error, {:http_error, 404, _}} ->
        :ok

      {:error, reason} ->
        Logger.error("S3 delete failed for s3://#{bucket}/#{key}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl true
  def exists?(path, opts) do
    {bucket, key} = parse_s3_path(path, opts)

    Logger.debug("S3 exists?: s3://#{bucket}/#{key}")

    ExAws.S3.head_object(bucket, key)
    |> ExAws.request(Config.ex_aws_config())
    |> case do
      {:ok, _response} -> true
      {:error, {:http_error, 404, _}} -> false
      {:error, _reason} -> false
    end
  end

  # Private helpers

  defp parse_s3_path(path, opts) do
    # Get base URL from opts if provided, otherwise assume path is full S3 path
    case Keyword.get(opts, :base_url) do
      nil ->
        # Path should be in format "s3://bucket/key"
        parse_full_s3_url(path)

      base_url ->
        # Combine base URL with relative path
        {bucket, base_prefix} = parse_full_s3_url(base_url)
        full_key = Path.join([base_prefix, path]) |> String.trim_leading("/")
        {bucket, full_key}
    end
  end

  defp parse_full_s3_url("s3://" <> rest) do
    case String.split(rest, "/", parts: 2) do
      [bucket] ->
        {bucket, ""}

      [bucket, key] ->
        {bucket, key}
    end
  end

  defp parse_full_s3_url(url) do
    raise ArgumentError, """
    Invalid S3 URL format: #{url}

    Expected format: s3://bucket-name or s3://bucket-name/path/prefix
    """
  end
end
