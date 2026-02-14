defmodule Iceberg.ConfigTest do
  use ExUnit.Case, async: true

  alias Iceberg.Config

  describe "storage_backend/1" do
    test "returns storage module from opts" do
      assert Config.storage_backend(storage: Iceberg.Storage.Memory) == Iceberg.Storage.Memory
    end

    test "raises ArgumentError when missing" do
      assert_raise ArgumentError, ~r/Iceberg configuration missing: storage/, fn ->
        Config.storage_backend([])
      end
    end
  end

  describe "compute_backend/1" do
    test "returns compute module from opts" do
      assert Config.compute_backend(compute: Iceberg.Test.MockCompute) ==
               Iceberg.Test.MockCompute
    end

    test "raises ArgumentError when missing" do
      assert_raise ArgumentError, ~r/Iceberg configuration missing: compute/, fn ->
        Config.compute_backend([])
      end
    end
  end

  describe "base_url/1" do
    test "returns base_url from opts" do
      assert Config.base_url(base_url: "s3://my-bucket") == "s3://my-bucket"
    end

    test "raises ArgumentError when missing" do
      assert_raise ArgumentError, ~r/Iceberg configuration missing: base_url/, fn ->
        Config.base_url([])
      end
    end
  end

  describe "full_url/2" do
    test "joins base and path" do
      opts = [base_url: "s3://bucket"]
      assert Config.full_url("data/file.parquet", opts) == "s3://bucket/data/file.parquet"
    end

    test "handles trailing slash on base" do
      opts = [base_url: "s3://bucket/"]
      assert Config.full_url("data/file.parquet", opts) == "s3://bucket/data/file.parquet"
    end

    test "handles leading slash on path" do
      opts = [base_url: "s3://bucket"]
      assert Config.full_url("/data/file.parquet", opts) == "s3://bucket/data/file.parquet"
    end

    test "handles both trailing and leading slashes" do
      opts = [base_url: "s3://bucket/"]
      assert Config.full_url("/data/file.parquet", opts) == "s3://bucket/data/file.parquet"
    end
  end

  describe "get/3" do
    test "returns value from opts" do
      assert Config.get(:my_key, [my_key: "value"], "default") == "value"
    end

    test "returns default when key missing" do
      assert Config.get(:my_key, [], "default") == "default"
    end
  end
end
