defmodule Iceberg.ParquetStatsTest do
  use ExUnit.Case

  alias Iceberg.ParquetStats
  alias Iceberg.Test.MockCompute

  @opts [compute: MockCompute, storage: Iceberg.Storage.Memory, base_url: "memory://test"]

  setup do
    MockCompute.clear()
    :ok
  end

  describe "extract/3" do
    test "delegates SQL to compute backend via mock" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "s3://bucket/table/data/file1.parquet",
            "file_size_in_bytes" => 1024,
            "record_count" => 100
          }
        ]
      })

      {:ok, stats} = ParquetStats.extract(:conn, "s3://bucket/table/data/**/*.parquet", @opts)

      assert length(stats) == 1
      [file] = stats
      assert file.file_path == "s3://bucket/table/data/file1.parquet"
      assert file.file_size_in_bytes == 1024
      assert file.record_count == 100
    end

    test "parses hive-style partition paths" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "s3://bucket/table/data/year=2024/month=1/day=15/file.parquet",
            "file_size_in_bytes" => 512,
            "record_count" => 50
          }
        ]
      })

      {:ok, [file]} = ParquetStats.extract(:conn, "s3://bucket/table/data/**/*.parquet", @opts)

      assert file.partition_values == %{
               "year" => "2024",
               "month" => "1",
               "day" => "15"
             }
    end

    test "converts numeric types to integers" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "test.parquet",
            "file_size_in_bytes" => 1024.0,
            "record_count" => 100.0
          }
        ]
      })

      {:ok, [file]} = ParquetStats.extract(:conn, "test/**/*.parquet", @opts)

      assert file.file_size_in_bytes == 1024
      assert file.record_count == 100
    end

    test "handles nil values" do
      MockCompute.set_response("parquet_metadata", {
        :ok,
        [
          %{
            "file_path" => "test.parquet",
            "file_size_in_bytes" => nil,
            "record_count" => nil
          }
        ]
      })

      {:ok, [file]} = ParquetStats.extract(:conn, "test/**/*.parquet", @opts)

      assert file.file_size_in_bytes == 0
      assert file.record_count == 0
    end

    test "returns error from compute backend" do
      MockCompute.set_response("parquet_metadata", {:error, :connection_failed})

      assert {:error, :connection_failed} =
               ParquetStats.extract(:conn, "test/**/*.parquet", @opts)
    end
  end

  describe "extract_column_stats/2" do
    test "returns empty stats (placeholder)" do
      {:ok, stats} = ParquetStats.extract_column_stats(:conn, "test.parquet")

      assert stats.column_sizes == nil
      assert stats.value_counts == nil
      assert stats.null_value_counts == nil
      assert stats.lower_bounds == nil
      assert stats.upper_bounds == nil
    end
  end
end
