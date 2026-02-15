# IcebergDuckdb

**Opinionated, production-ready DuckDB adapter for Apache Iceberg using ADBC.**

Provides a batteries-included experience for using Apache Iceberg tables with DuckDB via the high-performance ADBC (Arrow Database Connectivity) interface.

## Features

- ✅ **Batteries included** - Works out of the box with sensible defaults
- ✅ **High-performance ADBC** - Native Arrow integration for fast data transfer
- ✅ **Automatic S3 setup** - Reads credentials from application config
- ✅ **Safe defaults** - Memory limits and thread constraints for production use
- ✅ **Zero configuration** - Automatic Iceberg and httpfs extension loading
- ✅ **Flexible** - Override defaults per-connection when needed
- ✅ **Observable** - Built-in Telemetry events for monitoring and metrics

## Installation

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:iceberg, "~> 0.1"},
    {:iceberg_duckdb, "~> 0.1"}
  ]
end
```

## Quick Start

```elixir
# Configure in config/runtime.exs
config :iceberg_duckdb,
  aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
  aws_region: "us-east-1"

# Open connection (Iceberg support enabled by default)
{:ok, conn} = IcebergDuckdb.open()

# Use with Iceberg tables
opts = [
  storage: MyApp.S3Storage,
  compute: IcebergDuckdb,
  base_url: "s3://my-bucket"
]

{:ok, snapshot} = Iceberg.Table.insert_overwrite(
  conn,
  MyApp.Schema.Events,
  "SELECT * FROM staging_events",
  opts
)

# Close when done
IcebergDuckdb.close(conn)
```

## Configuration

### Application Config

Configure in your `config/runtime.exs`:

```elixir
config :iceberg_duckdb,
  # DuckDB settings
  memory_limit: "4GB",                  # Default: "2GB"
  threads: 8,                           # Default: number of CPU cores
  temp_dir: "/tmp/duckdb",              # Default: system temp
  
  # S3 credentials (pull from environment)
  aws_access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
  aws_region: "us-east-1",              # Default: "us-east-1"
  
  # For S3-compatible services (MinIO, R2, etc.)
  s3_endpoint: System.get_env("S3_ENDPOINT")
```

All settings are optional with sensible defaults.

### Per-Connection Overrides

```elixir
# Override defaults for specific connection
{:ok, conn} = IcebergDuckdb.open(
  iceberg: true,
  memory_limit: "8GB",    # Override for this connection
  threads: 16
)
```

## Usage Patterns

### In-Memory Database

```elixir
{:ok, conn} = IcebergDuckdb.open()
# Work with data
IcebergDuckdb.close(conn)
```

### Persistent Database

```elixir
{:ok, conn} = IcebergDuckdb.open("my_warehouse.db")
# Database file is created/opened
IcebergDuckdb.close(conn)
```

### With Connection Pool

```elixir
IcebergDuckdb.with_connection(fn conn ->
  Iceberg.Table.create(MySchema, nil, opts)
  # Connection is automatically closed
end)
```

## Architecture

This adapter implements the `Iceberg.Compute` behaviour and provides:

1. **Connection Management**: Opens/closes DuckDB connections via ADBC
2. **SQL Execution**: `query/2` and `execute/2` for running SQL
3. **Data Writing**: `write_data_files/4` with optimized Parquet COPY syntax
4. **Auto-Configuration**: Automatically sets up extensions and credentials

The adapter uses DuckDB-specific optimizations:
- `PARTITION_BY` for partitioned writes
- `PER_THREAD_OUTPUT` for unpartitioned tables
- `FILENAME_PATTERN` for consistent file naming
- `OVERWRITE_OR_IGNORE` for safe overwrites

## Performance Tips

- **Memory Limit**: Set to ~50-75% of available RAM for safety
- **Threads**: Usually best left at default (number of CPU cores)
- **Temp Directory**: Use fast local disk (NVMe) for large operations
- **S3**: The adapter enables `force_download` to avoid range request issues

## Observability

IcebergDuckdb emits [Telemetry](https://hexdocs.pm/telemetry/) events for monitoring, logging, and metrics.

See [TELEMETRY.md](TELEMETRY.md) for complete documentation.

### Quick Example

```elixir
# Attach a simple logger
:telemetry.attach(
  "iceberg-logger",
  [:iceberg_duckdb, :query, :stop],
  fn _event, measurements, metadata, _config ->
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)
    IO.puts("Query completed in #{duration_ms}ms")
  end,
  nil
)
```

### Phoenix LiveDashboard

Works automatically with Phoenix LiveDashboard - just define metrics:

```elixir
def metrics do
  [
    summary("iceberg_duckdb.query.duration", unit: {:native, :millisecond}),
    counter("iceberg_duckdb.write_data_files.count")
  ]
end
```

## Troubleshooting

**Native compilation fails:**
- ADBC requires a C compiler. Install build tools for your platform.
- On macOS: `xcode-select --install`
- On Ubuntu: `apt-get install build-essential`

**S3 access denied:**
- Verify AWS credentials are set correctly
- Check bucket permissions and IAM policies
- For MinIO/custom endpoints, ensure `S3_ENDPOINT` is set

**Out of memory:**
- Reduce `DUCKDB_MEMORY_LIMIT`
- Process data in smaller batches
- Use partitioned writes to reduce memory pressure

## License

Apache 2.0 - See LICENSE file for details.
