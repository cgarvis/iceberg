# IcebergS3

**Production-ready S3 storage adapter for Apache Iceberg.**

Provides a complete S3 implementation of the Iceberg storage interface using ExAws, enabling seamless integration with Amazon S3 and S3-compatible services.

## Features

- ✅ **Full Iceberg.Storage implementation** - All required operations (upload, download, list, delete, exists?)
- ✅ **AWS S3 native** - Built on ExAws for reliability and performance
- ✅ **S3-compatible services** - Works with MinIO, Cloudflare R2, DigitalOcean Spaces, etc.
- ✅ **Flexible authentication** - Environment variables, instance profiles, or explicit configuration
- ✅ **Proper error handling** - Clear error messages and idempotent operations
- ✅ **Production tested** - Used in production for managing Iceberg metadata and manifests

## Installation

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:iceberg, "~> 0.1"},
    {:iceberg_s3, "~> 0.1"}
  ]
end
```

## Quick Start

```elixir
# Configure in config/runtime.exs (optional - will use ExAws defaults if not provided)
config :iceberg_s3,
  access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
  region: "us-east-1"

# Use with Iceberg operations
opts = [
  storage: IcebergS3,
  compute: IcebergDuckdb,
  base_url: "s3://my-bucket/warehouse/events"
]

# Create a table
schema = Iceberg.Schema.new do
  field :id, :long, required: true
  field :timestamp, :timestamptz, required: true
  field :event_type, :string
end

{:ok, metadata} = Iceberg.Table.create(schema, nil, opts)

# Write data
{:ok, snapshot} = Iceberg.Table.insert_overwrite(
  conn,
  schema,
  "SELECT * FROM staging_events",
  opts
)
```

## Configuration

### Application Config

Configure in your `config/runtime.exs`:

```elixir
config :iceberg_s3,
  # AWS credentials (optional - uses ExAws credential chain if not provided)
  access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
  region: "us-east-1",              # Default: "us-east-1"
  
  # For S3-compatible services (MinIO, R2, etc.)
  endpoint: System.get_env("S3_ENDPOINT")
```

All settings are optional. If not provided, ExAws will use its default credential provider chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM instance profile (when running on EC2)

### Base URL Format

The `base_url` option determines where Iceberg data is stored:

```elixir
# Store at bucket root
base_url: "s3://my-bucket"

# Store under a prefix (recommended)
base_url: "s3://my-bucket/warehouse/production"
base_url: "s3://my-bucket/lakehouse/events"
```

All relative paths are appended to this base URL:
- Metadata: `{base_url}/metadata/v1.metadata.json`
- Manifests: `{base_url}/metadata/snap-123-1-abc.avro`
- Data: `{base_url}/data/part-00000.parquet` (written by compute engine)

## S3-Compatible Services

### MinIO

```elixir
config :iceberg_s3,
  endpoint: "http://localhost:9000",
  access_key_id: "minioadmin",
  secret_access_key: "minioadmin",
  region: "us-east-1"
```

### Cloudflare R2

```elixir
config :iceberg_s3,
  endpoint: "https://<account-id>.r2.cloudflarestorage.com",
  access_key_id: System.get_env("R2_ACCESS_KEY_ID"),
  secret_access_key: System.get_env("R2_SECRET_ACCESS_KEY"),
  region: "auto"
```

### DigitalOcean Spaces

```elixir
config :iceberg_s3,
  endpoint: "https://nyc3.digitaloceanspaces.com",
  access_key_id: System.get_env("SPACES_ACCESS_KEY"),
  secret_access_key: System.get_env("SPACES_SECRET_KEY"),
  region: "nyc3"
```

## Usage with Iceberg

### Creating Tables

```elixir
opts = [
  storage: IcebergS3,
  compute: IcebergDuckdb,
  base_url: "s3://my-bucket/warehouse"
]

schema = Iceberg.Schema.new do
  field :user_id, :long, required: true
  field :created_at, :timestamptz, required: true
  field :action, :string
end

# Creates metadata files in S3
{:ok, metadata} = Iceberg.Table.create(schema, nil, opts)
```

### Reading Metadata

```elixir
# Loads the latest metadata from S3
{:ok, metadata} = Iceberg.Metadata.load("warehouse/events", opts)
```

### Writing Data

```elixir
# DuckDB writes Parquet files directly to S3
# IcebergS3 handles metadata and manifest files
{:ok, snapshot} = Iceberg.Table.insert_overwrite(
  conn,
  schema,
  "SELECT * FROM source_data",
  opts
)
```

## Architecture

IcebergS3 implements the `Iceberg.Storage` behavior with five operations:

### `upload/3`
Uploads content to S3 with optional content type.

```elixir
IcebergS3.upload("metadata/v1.metadata.json", json_content, 
  content_type: "application/json", base_url: "s3://bucket")
```

### `download/1`
Downloads content from S3.

```elixir
{:ok, content} = IcebergS3.download("s3://bucket/metadata/v1.metadata.json")
{:error, :not_found} = IcebergS3.download("s3://bucket/missing.json")
```

### `list/1`
Lists all objects with a given prefix.

```elixir
files = IcebergS3.list("s3://bucket/data/")
# Returns: ["data/part-00000.parquet", "data/part-00001.parquet", ...]
```

### `delete/1`
Deletes an object (idempotent - returns `:ok` even if object doesn't exist).

```elixir
:ok = IcebergS3.delete("s3://bucket/data/old-file.parquet")
:ok = IcebergS3.delete("s3://bucket/nonexistent.parquet")  # Still returns :ok
```

### `exists?/1`
Checks if an object exists.

```elixir
true = IcebergS3.exists?("s3://bucket/metadata/v1.metadata.json")
false = IcebergS3.exists?("s3://bucket/missing.json")
```

## Division of Responsibilities

When using IcebergS3 with IcebergDuckdb:

**IcebergS3 handles:**
- Iceberg metadata JSON files (`v1.metadata.json`, `v2.metadata.json`, etc.)
- Manifest Avro files (`manifest-list.avro`, manifest files)
- Version hint file (`version-hint.text`)
- Table metadata operations (list, delete metadata files)

**IcebergDuckdb handles:**
- Writing Parquet data files to S3 (via DuckDB's native S3 support)
- Reading Parquet data files from S3 (via DuckDB's httpfs extension)
- S3 authentication for data operations (via DuckDB SECRET)

This separation provides the best performance - IcebergS3 handles small metadata files via ExAws, while DuckDB handles large data files with its optimized native implementation.

## Error Handling

IcebergS3 provides clear error responses:

```elixir
# File not found
{:error, :not_found} = IcebergS3.download("s3://bucket/missing.json")

# Bucket not found
{:error, :bucket_not_found} = IcebergS3.upload("data.json", content, 
  base_url: "s3://nonexistent-bucket")

# Other S3 errors
{:error, reason} = IcebergS3.download("s3://bucket/file.json")
```

All operations log errors with details for debugging.

## Performance Considerations

### Metadata Operations
- Metadata files are typically small (KB to low MB range)
- ExAws is optimized for these operations
- Operations are synchronous and blocking

### Data Operations
- Use IcebergDuckdb for data file operations (reading/writing Parquet)
- DuckDB's native S3 support is highly optimized for large files
- Parallel reads/writes, connection pooling, multipart uploads

### Best Practices
- Keep metadata files small (avoid huge schemas or partition lists)
- Use appropriate partitioning to limit manifest sizes
- Configure proper S3 lifecycle policies for data retention
- Use S3 versioning for critical metadata files

## Testing

Integration tests are included but skipped by default (require S3 access):

```bash
# Set up test S3 bucket
export TEST_S3_BUCKET=iceberg-test-bucket
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret

# Run tests (remove @moduletag :skip from test file)
cd packages/iceberg_s3
mix test
```

For unit tests, use the built-in `Iceberg.Storage.Memory` adapter instead.

## Troubleshooting

**S3 access denied:**
- Verify AWS credentials are configured correctly
- Check bucket exists and you have permissions (s3:GetObject, s3:PutObject, s3:DeleteObject, s3:ListBucket)
- For IAM roles, ensure the role is attached to your EC2 instance

**Invalid S3 URL format:**
- Ensure base_url starts with `s3://`
- Format: `s3://bucket-name` or `s3://bucket-name/prefix`
- Bucket names must follow AWS naming rules (lowercase, no underscores, etc.)

**Connection timeouts:**
- Check network connectivity to S3
- For S3-compatible services, verify endpoint URL is correct
- Consider increasing ExAws timeout configuration

**Bucket not found:**
- Verify bucket name is correct
- Ensure bucket exists in the configured region
- For S3-compatible services, check endpoint configuration

## License

Apache 2.0 - See LICENSE file for details.
