defmodule Iceberg.PartitionTransformsTest do
  use ExUnit.Case, async: true

  describe "simple partition transforms" do
    defmodule HourPartitioned do
      use Iceberg.Schema

      schema "test/hour_partitioned" do
        field(:timestamp, :timestamp, required: true)
        field(:data, :string)

        partition(hour(:timestamp))
      end
    end

    test "hour transform generates correct partition spec" do
      spec = HourPartitioned.__partition_spec__()

      assert spec["spec-id"] == 0
      assert length(spec["fields"]) == 1

      [partition_field] = spec["fields"]
      assert partition_field["name"] == "timestamp_hour"
      assert partition_field["transform"] == "hour"
      assert partition_field["source-id"] == 1
    end
  end

  describe "parameterized partition transforms" do
    defmodule BucketPartitioned do
      use Iceberg.Schema

      schema "test/bucket_partitioned" do
        field(:user_id, :string, required: true)
        field(:data, :string)

        partition(bucket(:user_id, 10))
      end
    end

    defmodule TruncatePartitioned do
      use Iceberg.Schema

      schema "test/truncate_partitioned" do
        field(:name, :string, required: true)
        field(:data, :string)

        partition(truncate(:name, 5))
      end
    end

    test "bucket transform with N parameter generates correct partition spec" do
      spec = BucketPartitioned.__partition_spec__()

      assert spec["spec-id"] == 0
      assert length(spec["fields"]) == 1

      [partition_field] = spec["fields"]
      assert partition_field["name"] == "user_id_bucket"
      assert partition_field["transform"] == "bucket[10]"
      assert partition_field["source-id"] == 1
    end

    test "truncate transform with W parameter generates correct partition spec" do
      spec = TruncatePartitioned.__partition_spec__()

      assert spec["spec-id"] == 0
      assert length(spec["fields"]) == 1

      [partition_field] = spec["fields"]
      assert partition_field["name"] == "name_truncate"
      assert partition_field["transform"] == "truncate[5]"
      assert partition_field["source-id"] == 1
    end
  end
end
