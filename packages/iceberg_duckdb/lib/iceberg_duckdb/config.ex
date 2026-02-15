defmodule IcebergDuckdb.Config do
  @moduledoc """
  Configuration for the IcebergDuckdb adapter.

  ## Configuration

  Configure via application config:

      config :iceberg_duckdb,
        memory_limit: "4GB",
        threads: 4,
        temp_dir: "/tmp/duckdb",
        aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
        aws_region: "us-east-1",
        s3_endpoint: System.get_env("S3_ENDPOINT")

  All configuration is optional and has sensible defaults.
  """

  @doc """
  Gets the DuckDB memory limit.

  Defaults to 2GB for safety in concurrent environments.
  """
  def memory_limit do
    Application.get_env(:iceberg_duckdb, :memory_limit, "2GB")
  end

  @doc """
  Gets the number of threads DuckDB should use.

  Defaults to the number of schedulers online (typically CPU cores).
  """
  def threads do
    Application.get_env(:iceberg_duckdb, :threads, :erlang.system_info(:schedulers_online))
  end

  @doc """
  Gets the temporary directory for DuckDB.

  Defaults to the system temporary directory.
  """
  def temp_dir do
    Application.get_env(:iceberg_duckdb, :temp_dir, System.tmp_dir!())
  end

  @doc """
  Gets AWS access key ID from application config.
  """
  def aws_access_key_id do
    Application.get_env(:iceberg_duckdb, :aws_access_key_id)
  end

  @doc """
  Gets AWS secret access key from application config.
  """
  def aws_secret_access_key do
    Application.get_env(:iceberg_duckdb, :aws_secret_access_key)
  end

  @doc """
  Gets AWS region.

  Defaults to us-east-1.
  """
  def aws_region do
    Application.get_env(:iceberg_duckdb, :aws_region, "us-east-1")
  end

  @doc """
  Gets custom S3 endpoint (for S3-compatible services like MinIO).
  """
  def s3_endpoint do
    Application.get_env(:iceberg_duckdb, :s3_endpoint)
  end
end
