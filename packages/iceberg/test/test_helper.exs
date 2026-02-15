{:ok, _} = Iceberg.Storage.Memory.start_link()
{:ok, _} = Iceberg.Test.MockCompute.start_link()

ExUnit.start(exclude: [:duckdb])
