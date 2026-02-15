defmodule IcebergS3.Config do
  @moduledoc """
  Configuration for the IcebergS3 storage adapter.

  ## Configuration

  Configure via application config:

      config :iceberg_s3,
        access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
        region: "us-east-1",
        endpoint: System.get_env("S3_ENDPOINT")

  All configuration is optional. If not provided via application config,
  ExAws will use its default credential chain (environment variables, instance metadata, etc.).
  """

  @doc """
  Gets AWS access key ID from application config.
  """
  def access_key_id do
    Application.get_env(:iceberg_s3, :access_key_id)
  end

  @doc """
  Gets AWS secret access key from application config.
  """
  def secret_access_key do
    Application.get_env(:iceberg_s3, :secret_access_key)
  end

  @doc """
  Gets AWS region.

  Defaults to us-east-1.
  """
  def region do
    Application.get_env(:iceberg_s3, :region, "us-east-1")
  end

  @doc """
  Gets custom S3 endpoint (for S3-compatible services like MinIO, Cloudflare R2, etc.).
  """
  def endpoint do
    Application.get_env(:iceberg_s3, :endpoint)
  end

  @doc """
  Builds ExAws configuration from application config.
  """
  def ex_aws_config do
    base_config = [
      region: region()
    ]

    base_config
    |> maybe_put(:access_key_id, access_key_id())
    |> maybe_put(:secret_access_key, secret_access_key())
    |> maybe_put(:endpoint, endpoint())
  end

  defp maybe_put(config, _key, nil), do: config
  defp maybe_put(config, key, value), do: Keyword.put(config, key, value)
end
