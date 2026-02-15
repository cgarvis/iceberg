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

  describe "insert_overwrite/4" do
    test "handles unpartitioned tables without syntax errors" do
      # Create unpartitioned table (Simple schema has no partitions)
      :ok = Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)

      # Mock successful execution - the COPY statement should be well-formed
      MockCompute.set_response("COPY", {:ok, :executed})
      MockCompute.set_response("parquet_metadata", {:ok, []})

      # This should execute without syntax errors
      assert {:ok, _snapshot} =
               Table.insert_overwrite(
                 :mock_conn,
                 Iceberg.Test.Schemas.Simple,
                 "SELECT '1' AS id, 'test' AS name, 100 AS count",
                 @opts
               )
    end

    test "handles partitioned tables correctly" do
      # Create partitioned table (Events schema has day partition)
      :ok = Table.create(Iceberg.Test.Schemas.Events, nil, @opts)

      # Mock successful execution
      MockCompute.set_response("COPY", {:ok, :executed})
      MockCompute.set_response("parquet_metadata", {:ok, []})

      # This should work with PARTITION_BY clause
      assert {:ok, _snapshot} =
               Table.insert_overwrite(
                 :mock_conn,
                 Iceberg.Test.Schemas.Events,
                 "SELECT '1' AS id, NOW() AS timestamp, 'test' AS event_type, '{}' AS data, CURRENT_DATE AS partition_date",
                 @opts
               )
    end
  end

  describe "add_column/3" do
    test "adds a new column to table schema" do
      # Create table
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      # Add column
      assert :ok =
               Table.add_column(
                 "test/table",
                 %{name: "email", type: "string", required: false},
                 Keyword.merge(@opts, table_empty?: true)
               )

      # Verify column was added
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_schema = Iceberg.Metadata.get_current_schema(metadata)
      assert length(current_schema["fields"]) == 2
      email_field = Enum.find(current_schema["fields"], fn f -> f["name"] == "email" end)
      assert email_field["type"] == "string"
    end

    test "works with schema module" do
      :ok = Table.create(Iceberg.Test.Schemas.Simple, nil, @opts)

      assert :ok =
               Table.add_column(
                 Iceberg.Test.Schemas.Simple,
                 %{name: "email", type: "string"},
                 Keyword.merge(@opts, table_empty?: true)
               )
    end

    test "returns error for duplicate column name" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:error, message} =
               Table.add_column(
                 "test/table",
                 %{name: "id", type: "string"},
                 Keyword.merge(@opts, table_empty?: true)
               )

      assert message =~ "Column 'id' already exists"
    end

    test "returns warnings in permissive mode" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:ok, warnings} =
               Table.add_column(
                 "test/table",
                 %{name: "required_field", type: "string", required: true},
                 Keyword.merge(@opts, mode: :permissive, table_empty?: false)
               )

      assert warnings =~ "Adding required column"
    end
  end

  describe "drop_column/3" do
    test "drops a column from table schema" do
      # Create table with multiple columns
      schema = [{:id, "STRING", true}, {:temp, "STRING", false}]
      :ok = Table.create("test/table", schema, @opts)

      # Drop column
      assert :ok = Table.drop_column("test/table", "temp", @opts)

      # Verify column was dropped
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_schema = Iceberg.Metadata.get_current_schema(metadata)
      assert length(current_schema["fields"]) == 1
      assert hd(current_schema["fields"])["name"] == "id"
    end

    test "rejects dropping required column in strict mode" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:error, message} = Table.drop_column("test/table", "id", @opts)
      assert message =~ "Cannot drop required column"
    end

    test "allows dropping required column in permissive mode" do
      schema = [{:id, "STRING", true}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:ok, warnings} =
               Table.drop_column("test/table", "id", Keyword.merge(@opts, mode: :permissive))

      assert warnings =~ "Dropping required column"
    end
  end

  describe "rename_column/4" do
    test "renames a column in table schema" do
      schema = [{:old_name, "STRING", false}]
      :ok = Table.create("test/table", schema, @opts)

      # Rename column
      assert :ok = Table.rename_column("test/table", "old_name", "new_name", @opts)

      # Verify column was renamed
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_schema = Iceberg.Metadata.get_current_schema(metadata)
      assert hd(current_schema["fields"])["name"] == "new_name"
      assert hd(current_schema["fields"])["id"] == 1
    end

    test "rejects renaming to existing column name" do
      schema = [{:field1, "STRING", false}, {:field2, "STRING", false}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:error, message} = Table.rename_column("test/table", "field1", "field2", @opts)
      assert message =~ "Column 'field2' already exists"
    end
  end

  describe "update_column_type/4" do
    test "updates column type with safe promotion" do
      schema = [{:count, "INT", false}]
      :ok = Table.create("test/table", schema, @opts)

      # Promote int to long
      assert :ok = Table.update_column_type("test/table", "count", "long", @opts)

      # Verify type was updated
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_schema = Iceberg.Metadata.get_current_schema(metadata)
      assert hd(current_schema["fields"])["type"] == "long"
    end

    test "rejects unsafe type changes in strict mode" do
      schema = [{:count, "LONG", false}]
      :ok = Table.create("test/table", schema, @opts)

      assert {:error, message} = Table.update_column_type("test/table", "count", "int", @opts)
      assert message =~ "is not a safe promotion"
    end
  end

  describe "evolve_schema/3 - integration" do
    test "applies complex schema evolution with multiple operations" do
      # Create table with initial schema
      schema = [{:id, "STRING", true}, {:count, "INT", false}, {:old_name, "STRING", false}]
      :ok = Table.create("test/table", schema, @opts)

      # Evolution 1: Add column
      assert :ok =
               Table.add_column(
                 "test/table",
                 %{name: "email", type: "string"},
                 Keyword.merge(@opts, table_empty?: true)
               )

      # Evolution 2: Rename column
      assert :ok = Table.rename_column("test/table", "old_name", "new_name", @opts)

      # Evolution 3: Update type
      assert :ok = Table.update_column_type("test/table", "count", "long", @opts)

      # Verify final state
      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_schema = Iceberg.Metadata.get_current_schema(metadata)

      # Should have 4 fields
      assert length(current_schema["fields"]) == 4

      # Verify each field
      field_names = Enum.map(current_schema["fields"], & &1["name"])
      assert "id" in field_names
      assert "email" in field_names
      assert "new_name" in field_names
      refute "old_name" in field_names

      count_field = Enum.find(current_schema["fields"], fn f -> f["name"] == "count" end)
      assert count_field["type"] == "long"

      # Should have created 4 schema versions (initial + 3 evolutions)
      assert length(metadata["schemas"]) == 4
      assert metadata["current-schema-id"] == 3
    end

    test "preserves field IDs across evolutions" do
      schema = [{:field1, "STRING", false}, {:field2, "STRING", false}]
      :ok = Table.create("test/table", schema, @opts)

      # Drop field2
      :ok = Table.drop_column("test/table", "field2", Keyword.merge(@opts, force: true))

      # Add new field - should get ID 3, not 2
      :ok = Table.add_column("test/table", %{name: "field3", type: "string"}, @opts)

      {:ok, metadata} = Iceberg.Metadata.load("test/table", @opts)
      current_schema = Iceberg.Metadata.get_current_schema(metadata)

      field3 = Enum.find(current_schema["fields"], fn f -> f["name"] == "field3" end)
      assert field3["id"] == 3

      # Verify last-column-id tracks highest ID ever used
      assert metadata["last-column-id"] == 3
    end
  end
end
