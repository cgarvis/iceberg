defmodule Iceberg do
  @moduledoc """
  Apache Iceberg v2 table format implementation in pure Elixir.

  Provides a complete Iceberg v2 table format with:
  - Avro Object Container File encoding (no external dependencies)
  - Manifest and manifest-list creation
  - Snapshot management with proper metadata
  - Ecto-like schema DSL for type safety
  - Pluggable storage and compute backends

  ## Quick Start

  Define a schema:

      defmodule MyApp.Schemas.Events do
        use Iceberg.Schema

        schema "canonical/events" do
          field :id, :string, required: true
          field :timestamp, :timestamp, required: true
          field :data, :string

          partition day(:timestamp)
        end
      end

  Create a table:

      opts = [
        storage: MyApp.S3Storage,
        compute: MyApp.DuckDBCompute,
        base_url: "s3://my-bucket"
      ]

      Iceberg.create(MyApp.Schemas.Events, nil, opts)

  ## Architecture

  - `Iceberg.Schema` - Ecto-like DSL for defining table schemas
  - `Iceberg.Table` - Public API for table operations
  - `Iceberg.Metadata` - Metadata management (v{N}.metadata.json)
  - `Iceberg.Snapshot` - Snapshot orchestration
  - `Iceberg.Manifest` - Manifest file creation (Avro)
  - `Iceberg.ManifestList` - Manifest-list creation (Avro)
  - `Iceberg.Avro.Encoder` - Avro Object Container File encoder
  - `Iceberg.Storage` - Storage behaviour (S3, GCS, local, etc.)
  - `Iceberg.Compute` - Compute behaviour (DuckDB, Spark, Trino, etc.)
  """

  defdelegate create(schema_module_or_path, schema \\ nil, opts \\ []), to: Iceberg.Table

  defdelegate exists?(schema_module_or_path, opts \\ []), to: Iceberg.Table

  defdelegate insert_overwrite(conn, schema_module_or_path, source_query, opts \\ []),
    to: Iceberg.Table

  defdelegate ensure_name_mapping(schema_module_or_path, opts \\ []), to: Iceberg.Table
end
