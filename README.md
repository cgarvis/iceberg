# Iceberg for Elixir

**Apache Iceberg table format implementation for Elixir with production-ready adapters.**

This is a monorepo workspace containing the core Iceberg library and companion packages for working with Apache Iceberg tables in Elixir.

## Packages

### [`iceberg`](packages/iceberg) - Core Library

Pure Elixir implementation of the Apache Iceberg v2 table format specification.

- ✅ **Schema Evolution** - Add, drop, rename columns with DuckDB compatibility
- ✅ **Zero Dependencies** - Pure Elixir with built-in JSON/Avro encoding
- ✅ **Type Safety** - Ecto-like schema DSL with compile-time validation
- ✅ **Pluggable Backends** - Bring your own storage and compute
- ✅ **V1 Complete** - Full support for Iceberg format V1 operations

[📚 Full Documentation →](packages/iceberg/README.md)

### [`iceberg_duckdb`](packages/iceberg_duckdb) - DuckDB Adapter

Production-ready DuckDB adapter using high-performance ADBC.

- ✅ **Batteries Included** - Works out of the box with sensible defaults
- ✅ **High Performance** - Native Arrow integration via ADBC
- ✅ **Auto Configuration** - Automatic S3 setup and extension loading
- ✅ **Observable** - Built-in Telemetry events for monitoring

[📚 Full Documentation →](packages/iceberg_duckdb/README.md)

## Quick Start

Add to your `mix.exs`:

```elixir
def deps do
  [
    {:iceberg, "~> 0.1"},
    {:iceberg_duckdb, "~> 0.1"}  # Optional: DuckDB adapter
  ]
end
```

Define a schema:

```elixir
defmodule MyApp.Schemas.Events do
  use Iceberg.Schema

  schema "events" do
    field :id, :string, required: true
    field :timestamp, :timestamp, required: true
    field :event_type, :string
    
    partition day(:timestamp)
  end
end
```

Create a table and insert data:

```elixir
# Configure
opts = [
  storage: MyApp.S3Storage,
  compute: IcebergDuckdb,
  base_url: "s3://my-bucket"
]

# Open DuckDB connection
{:ok, conn} = IcebergDuckdb.open()

# Create table
{:ok, metadata} = Iceberg.Table.create(MyApp.Schemas.Events, conn, opts)

# Insert data
{:ok, snapshot} = Iceberg.Table.insert_overwrite(
  conn,
  MyApp.Schemas.Events,
  "SELECT * FROM staging_events",
  opts
)

# Evolve schema
{:ok, _metadata} = Iceberg.Table.add_column(
  MyApp.Schemas.Events,
  "user_agent",
  :string,
  opts
)
```

## Features

### Core Library (`iceberg`)

- **Schema Evolution**: Add, drop, rename columns with historical tracking
- **Snapshot Management**: Proper metadata versioning per Iceberg spec
- **Avro Encoding**: Zero-dependency manifest serialization
- **Type System**: All Iceberg primitive types supported
- **Partitioning**: Identity, year, month, day, hour transforms
- **Validation**: Three modes - strict, permissive, none

### DuckDB Adapter (`iceberg_duckdb`)

- **ADBC Integration**: High-performance Arrow-based connectivity
- **S3 Support**: Automatic credential configuration
- **Memory Safety**: Configurable limits for production use
- **Monitoring**: Telemetry events for observability
- **Optimized Writes**: Smart partitioned/unpartitioned strategies

## Development

This is a Mix workspace with shared tooling across packages.

### Setup

```bash
# Clone the repository
git clone https://github.com/cgarvis/iceberg.git
cd iceberg

# Install workspace dependencies
mix deps.get

# Install all package dependencies
mix deps.get_all
```

### Testing

```bash
# Test all packages
mix test_all

# Test specific package
cd packages/iceberg && mix test
cd packages/iceberg_duckdb && mix test

# Run with DuckDB integration tests
cd packages/iceberg && mix test --include duckdb
```

### Code Quality

```bash
# Format all packages
mix format_all

# Or format specific package
cd packages/iceberg && mix format

# Run quality checks (credo + dialyzer)
cd packages/iceberg && mix precommit
```

### Project Structure

```
iceberg/
├── mix.exs                          # Workspace configuration
├── mix.lock                         # Workspace dependencies
├── packages/
│   ├── iceberg/                     # Core library
│   │   ├── lib/
│   │   ├── test/
│   │   ├── mix.exs
│   │   └── README.md
│   └── iceberg_duckdb/              # DuckDB adapter
│       ├── lib/
│       ├── mix.exs
│       └── README.md
└── README.md                        # This file
```

## Documentation

- [Core Library Documentation](packages/iceberg/README.md)
- [DuckDB Adapter Documentation](packages/iceberg_duckdb/README.md)
- [Telemetry Events](packages/iceberg_duckdb/TELEMETRY.md)
- [Changelog](CHANGELOG.md)

## Roadmap

### V1 Features (Complete ✅)
- [x] Schema evolution (add, drop, rename columns)
- [x] Type promotions (int→long, float→double)
- [x] Field ID tracking and reuse prevention
- [x] Name mapping for historical column names
- [x] DuckDB compatibility verification

### V2 Features (Planned)
- [ ] Partition evolution
- [ ] Column defaults
- [ ] Sort order specification
- [ ] Row-level deletes
- [ ] Merge-on-read support

### Additional Features (Planned)
- [ ] ClickHouse adapter
- [ ] Spark adapter
- [ ] Time travel queries
- [ ] Table maintenance operations (compaction, expiry)

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes with conventional commits
4. Push to your branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- All packages must pass `mix test` and `mix precommit`
- Follow existing code style and conventions
- Add tests for new functionality
- Update documentation as needed
- Use conventional commit messages

## License

Apache 2.0 - See [LICENSE](LICENSE) file for details.

## Acknowledgments

Built to implement the [Apache Iceberg](https://iceberg.apache.org/) table format specification.
