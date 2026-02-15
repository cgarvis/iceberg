# Iceberg

Apache Iceberg v2 table format implementation in pure Elixir.

- Avro Object Container File encoding (zero runtime dependencies)
- Manifest and manifest-list creation per Iceberg v2 spec
- Snapshot management with proper metadata versioning
- Ecto-like schema DSL for type safety
- Pluggable storage and compute backends

## Installation

Add `iceberg` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:iceberg, "~> 0.1.0"}
  ]
end
```

Requires Elixir ~> 1.18 (uses the built-in `JSON` module).

## Quick Start

### 1. Define a Schema

```elixir
defmodule MyApp.Schemas.Events do
  use Iceberg.Schema

  schema "canonical/events" do
    field :id, :string, required: true
    field :timestamp, :timestamp, required: true
    field :event_type, :string
    field :data, :string
    field :partition_date, :date, required: true

    partition day(:partition_date)
  end
end
```

### 2. Implement Storage and Compute Backends

```elixir
defmodule MyApp.S3Storage do
  @behaviour Iceberg.Storage

  @impl true
  def upload(path, content, opts), do: # your S3 upload logic

  @impl true
  def download(path), do: # your S3 download logic

  @impl true
  def list(prefix), do: # list objects

  @impl true
  def delete(path), do: # delete object

  @impl true
  def exists?(path), do: # check existence
end
```

### 3. Create a Table

```elixir
opts = [
  storage: MyApp.S3Storage,
  compute: MyApp.DuckDBCompute,
  base_url: "s3://my-bucket"
]

Iceberg.create(MyApp.Schemas.Events, nil, opts)
```

### 4. Insert Data

```elixir
Iceberg.insert_overwrite(conn, MyApp.Schemas.Events, "SELECT * FROM staging", opts)
```

## Schema DSL

### Field Types

| DSL Type     | Iceberg Type | SQL Type  |
|--------------|--------------|-----------|
| `:string`    | string       | STRING    |
| `:long`      | long         | BIGINT    |
| `:int`       | int          | INTEGER   |
| `:double`    | double       | DOUBLE    |
| `:float`     | float        | FLOAT     |
| `:boolean`   | boolean      | BOOLEAN   |
| `:timestamp` | timestamp    | TIMESTAMP |
| `:date`      | date         | DATE      |
| `:binary`    | binary       | BINARY    |

### Partition Transforms

```elixir
partition day(:timestamp)     # Day-level partitioning
partition month(:date_col)    # Month-level partitioning
partition year(:date_col)     # Year-level partitioning
partition identity(:category) # Identity partitioning
```

## Architecture

```
lib/iceberg/
├── avro/
│   └── encoder.ex           # Avro Object Container File encoder
├── compute.ex               # Compute behaviour
├── config.ex                # Explicit opts-based configuration
├── manifest.ex              # Manifest file creation (Avro)
├── manifest_list.ex         # Manifest-list creation (Avro)
├── metadata.ex              # Metadata management (v{N}.metadata.json)
├── parquet_stats.ex         # Parquet statistics extraction
├── schema.ex                # Ecto-like schema DSL
├── snapshot.ex              # Snapshot orchestration
├── storage.ex               # Storage behaviour
├── storage/
│   └── memory.ex            # In-memory storage for testing
└── table.ex                 # Public API
```

## Testing

An in-memory storage backend is included for testing:

```elixir
{:ok, _} = Iceberg.Storage.Memory.start_link()

opts = [
  storage: Iceberg.Storage.Memory,
  base_url: "memory://test"
]

Iceberg.create(MyApp.Schemas.Events, nil, opts)
```

## References

- [Apache Iceberg Format Spec v2](https://iceberg.apache.org/spec/)
- [Apache Avro 1.11 Spec](https://avro.apache.org/docs/1.11.1/specification/)

## License

Apache-2.0
