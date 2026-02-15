# Telemetry Events

`IcebergDuckdb` emits [Telemetry](https://hexdocs.pm/telemetry/) events for observability and monitoring.

## Events

All events are prefixed with `[:iceberg_duckdb]`.

### Connection Events

#### `[:iceberg_duckdb, :connection, :open, :start]`

Emitted when a connection is being opened.

**Measurements:**
- `:system_time` - System time when the operation started

**Metadata:**
- `:path` - Database file path (nil for in-memory)
- `:memory_limit` - Configured memory limit
- `:threads` - Number of threads
- `:iceberg` - Whether Iceberg extension is enabled

#### `[:iceberg_duckdb, :connection, :open, :stop]`

Emitted when a connection is successfully opened.

**Measurements:**
- `:duration` - Time taken to open connection (native time unit)

**Metadata:**
- Same as `:start` event

#### `[:iceberg_duckdb, :connection, :open, :exception]`

Emitted when connection opening fails.

**Measurements:**
- `:duration` - Time taken before failure (native time unit)

**Metadata:**
- Same as `:start` event, plus:
- `:reason` - Error reason

### Query Events

#### `[:iceberg_duckdb, :query, :start]`

Emitted when a query starts executing.

**Measurements:**
- `:system_time` - System time when query started

**Metadata:**
- `:sql` - SQL query string

#### `[:iceberg_duckdb, :query, :stop]`

Emitted when a query completes successfully.

**Measurements:**
- `:duration` - Query execution time (native time unit)
- `:row_count` - Number of rows returned

**Metadata:**
- `:sql` - SQL query string

#### `[:iceberg_duckdb, :query, :exception]`

Emitted when a query fails.

**Measurements:**
- `:duration` - Time before failure (native time unit)

**Metadata:**
- `:sql` - SQL query string
- `:reason` - Error reason

### Execute Events

#### `[:iceberg_duckdb, :execute, :start]`

Emitted when a command starts executing.

**Measurements:**
- `:system_time` - System time when command started

**Metadata:**
- `:sql` - SQL command string

#### `[:iceberg_duckdb, :execute, :stop]`

Emitted when a command completes successfully.

**Measurements:**
- `:duration` - Command execution time (native time unit)

**Metadata:**
- `:sql` - SQL command string

#### `[:iceberg_duckdb, :execute, :exception]`

Emitted when a command fails.

**Measurements:**
- `:duration` - Time before failure (native time unit)

**Metadata:**
- `:sql` - SQL command string
- `:reason` - Error reason

### Write Data Files Events

#### `[:iceberg_duckdb, :write_data_files, :start]`

Emitted when starting to write Parquet data files.

**Measurements:**
- `:system_time` - System time when write started

**Metadata:**
- `:dest_path` - Destination path
- `:partitioned` - Boolean, whether data is partitioned
- `:partition_count` - Number of partition columns

#### `[:iceberg_duckdb, :write_data_files, :stop]`

Emitted when data files are successfully written.

**Measurements:**
- `:duration` - Write operation time (native time unit)

**Metadata:**
- Same as `:start` event

#### `[:iceberg_duckdb, :write_data_files, :exception]`

Emitted when writing data files fails.

**Measurements:**
- `:duration` - Time before failure (native time unit)

**Metadata:**
- Same as `:start` event, plus:
- `:reason` - Error reason

## Usage Examples

### Simple Logging

```elixir
# In your application.ex
defmodule MyApp.Application do
  def start(_type, _args) do
    :telemetry.attach_many(
      "iceberg-duckdb-logger",
      [
        [:iceberg_duckdb, :query, :stop],
        [:iceberg_duckdb, :write_data_files, :stop]
      ],
      &MyApp.Telemetry.handle_event/4,
      nil
    )
    
    # ...
  end
end

# In lib/my_app/telemetry.ex
defmodule MyApp.Telemetry do
  require Logger

  def handle_event([:iceberg_duckdb, :query, :stop], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)
    Logger.info("Query completed in #{duration_ms}ms, returned #{measurements.row_count} rows")
  end

  def handle_event([:iceberg_duckdb, :write_data_files, :stop], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)
    Logger.info("Wrote data files to #{metadata.dest_path} in #{duration_ms}ms")
  end
end
```

### Performance Monitoring with Telemetry.Metrics

```elixir
# In your application.ex or telemetry.ex
defmodule MyApp.Telemetry do
  import Telemetry.Metrics

  def metrics do
    [
      # Connection metrics
      summary("iceberg_duckdb.connection.open.duration",
        unit: {:native, :millisecond},
        tags: [:path, :iceberg]
      ),
      
      # Query metrics
      counter("iceberg_duckdb.query.count"),
      summary("iceberg_duckdb.query.duration",
        unit: {:native, :millisecond},
        tags: []
      ),
      distribution("iceberg_duckdb.query.row_count",
        buckets: [0, 10, 100, 1000, 10000, 100000]
      ),
      
      # Write metrics
      summary("iceberg_duckdb.write_data_files.duration",
        unit: {:native, :millisecond},
        tags: [:partitioned]
      ),
      
      # Error tracking
      counter("iceberg_duckdb.query.exception.count"),
      counter("iceberg_duckdb.write_data_files.exception.count")
    ]
  end
end
```

### Phoenix LiveDashboard Integration

```elixir
# In your router.ex
live_dashboard "/dashboard",
  metrics: MyApp.Telemetry

# Metrics will automatically appear in LiveDashboard
```

### Slow Query Logging

```elixir
defmodule MyApp.Telemetry do
  require Logger

  def handle_event([:iceberg_duckdb, :query, :stop], measurements, metadata, _config) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)
    
    if duration_ms > 1000 do
      Logger.warn("Slow query (#{duration_ms}ms): #{inspect(metadata.sql)}")
    end
  end
end
```

### APM Integration (New Relic, DataDog, etc.)

Most APM tools automatically listen to Telemetry events. Just install their packages:

```elixir
# mix.exs
{:new_relic_agent, "~> 1.0"}  # Automatically captures telemetry
```

## Converting Time Units

Telemetry uses native time units for measurements. Convert them:

```elixir
# To milliseconds
ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

# To microseconds
us = System.convert_time_unit(measurements.duration, :native, :microsecond)

# To seconds
sec = System.convert_time_unit(measurements.duration, :native, :second)
```
