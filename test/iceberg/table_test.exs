defmodule Iceberg.TableTest do
  use ExUnit.Case

  alias Iceberg.Storage.Memory
  alias Iceberg.Table
  alias Iceberg.Test.MockCompute

  @opts [
    storage: Memory,
    compute: MockCompute,
    base_url: "memory://test"
  ]

  setup do
    Memory.clear()
    MockCompute.clear()
    :ok
  end

  describe "create/3" do
    test "stores metadata files and returns :ok" do
      schema = [
        {:id, "STRING", true},
        {:name, "STRING", false}
      ]

      assert :ok = Table.create("test/table", schema, @opts)

      # Should have stored metadata files
      files = Memory.dump()
      paths = Map.keys(files)

      assert Enum.any?(paths, &String.contains?(&1, "metadata.json"))
      assert Enum.any?(paths, &String.contains?(&1, "version-hint.text"))
    end

    test "with schema module stores metadata" do
      assert :ok = Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)

      files = Memory.dump()
      paths = Map.keys(files)

      assert Enum.any?(paths, &String.contains?(&1, "metadata.json"))
    end

    test "duplicate create returns {:error, :already_exists}" do
      schema = [{:id, "STRING", true}]

      assert :ok = Table.create("test/table", schema, @opts)
      assert {:error, :already_exists} = Table.create("test/table", schema, @opts)
    end
  end

  describe "exists?/2" do
    test "returns false for non-existent table" do
      refute Table.exists?("nonexistent", @opts)
    end

    test "returns true for existing table" do
      Table.create("test/table", [{:id, "STRING", true}], @opts)
      assert Table.exists?("test/table", @opts)
    end

    test "works with schema module" do
      Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)
      assert Table.exists?(Iceberg.Test.Schemas.Simple, @opts)
    end
  end

  describe "ensure_name_mapping/2" do
    test "adds name-mapping property" do
      schema = [
        {:id, "STRING", true},
        {:name, "STRING", false}
      ]

      :ok = Table.create("test/table", schema, @opts)
      :ok = Table.ensure_name_mapping("test/table", @opts)

      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      assert Map.has_key?(metadata["properties"], "schema.name-mapping.default")
    end

    test "does not overwrite existing mapping" do
      :ok = Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)

      # Already has name mapping from create
      :ok = Table.ensure_name_mapping(Iceberg.Test.Schemas.Simple, @opts)
    end
  end
end
