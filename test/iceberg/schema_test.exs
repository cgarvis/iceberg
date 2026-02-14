defmodule Iceberg.SchemaTest do
  use ExUnit.Case, async: true

  alias Iceberg.Test.Schemas.{Events, Simple, AllPrimitiveTypes}

  describe "schema DSL" do
    test "compiles and generates __table_path__" do
      assert Events.__table_path__() == "canonical/events"
      assert Simple.__table_path__() == "canonical/simple"
    end

    test "__schema__/0 produces correct Iceberg v2 field structure" do
      schema = Events.__schema__()

      assert schema["type"] == "struct"
      assert schema["schema-id"] == 0

      fields = schema["fields"]
      assert length(fields) == 5

      # Fields have sequential IDs starting from 1
      assert Enum.map(fields, & &1["id"]) == [1, 2, 3, 4, 5]

      # Check field types
      id_field = Enum.find(fields, &(&1["name"] == "id"))
      assert id_field["type"] == "string"
      assert id_field["required"] == true

      timestamp_field = Enum.find(fields, &(&1["name"] == "timestamp"))
      assert timestamp_field["type"] == "timestamp"
      assert timestamp_field["required"] == true

      data_field = Enum.find(fields, &(&1["name"] == "data"))
      assert data_field["type"] == "string"
      assert data_field["required"] == false
    end

    test "__schema__/0 for unpartitioned schema" do
      schema = Simple.__schema__()
      fields = schema["fields"]

      assert length(fields) == 3
      assert Enum.map(fields, & &1["name"]) == ["id", "name", "count"]

      count_field = Enum.find(fields, &(&1["name"] == "count"))
      assert count_field["type"] == "long"
    end

    test "__partition_spec__/0 produces correct transforms for partitioned schema" do
      spec = Events.__partition_spec__()

      assert spec["spec-id"] == 0
      assert length(spec["fields"]) == 1

      [partition_field] = spec["fields"]
      assert partition_field["name"] == "partition_date_day"
      assert partition_field["transform"] == "day"
      assert partition_field["source-id"] == 5

      # Partition field-id is 1000 + source_field_index
      assert partition_field["field-id"] == 1000 + 4
    end

    test "__partition_spec__/0 returns empty fields for unpartitioned schema" do
      spec = Simple.__partition_spec__()

      assert spec["spec-id"] == 0
      assert spec["fields"] == []
    end

    test "__fields__/0 returns field tuples" do
      fields = Events.__fields__()
      assert length(fields) == 5

      assert {:id, :string, true} = Enum.at(fields, 0)
      assert {:timestamp, :timestamp, true} = Enum.at(fields, 1)
      assert {:data, :string, false} = Enum.at(fields, 3)
    end

    test "__partition_field__/0 returns partition field name" do
      assert Events.__partition_field__() == :partition_date
      assert Simple.__partition_field__() == nil
    end

    test "schema with all V1 primitive types compiles correctly" do
      schema = AllPrimitiveTypes.__schema__()

      assert schema["type"] == "struct"
      assert schema["schema-id"] == 0

      fields = schema["fields"]
      assert length(fields) == 14

      # Verify each type is correctly mapped
      assert Enum.find(fields, &(&1["name"] == "str_field"))["type"] == "string"
      assert Enum.find(fields, &(&1["name"] == "int_field"))["type"] == "int"
      assert Enum.find(fields, &(&1["name"] == "long_field"))["type"] == "long"
      assert Enum.find(fields, &(&1["name"] == "float_field"))["type"] == "float"
      assert Enum.find(fields, &(&1["name"] == "double_field"))["type"] == "double"
      assert Enum.find(fields, &(&1["name"] == "boolean_field"))["type"] == "boolean"
      assert Enum.find(fields, &(&1["name"] == "date_field"))["type"] == "date"
      assert Enum.find(fields, &(&1["name"] == "time_field"))["type"] == "time"
      assert Enum.find(fields, &(&1["name"] == "timestamp_field"))["type"] == "timestamp"
      assert Enum.find(fields, &(&1["name"] == "timestamptz_field"))["type"] == "timestamptz"
      assert Enum.find(fields, &(&1["name"] == "uuid_field"))["type"] == "uuid"
      assert Enum.find(fields, &(&1["name"] == "binary_field"))["type"] == "binary"
      assert Enum.find(fields, &(&1["name"] == "decimal_field"))["type"] == "decimal(10, 2)"
      assert Enum.find(fields, &(&1["name"] == "fixed_field"))["type"] == "fixed[16]"

      # Verify required flag works
      str_field = Enum.find(fields, &(&1["name"] == "str_field"))
      assert str_field["required"] == true

      int_field = Enum.find(fields, &(&1["name"] == "int_field"))
      assert int_field["required"] == false
    end
  end

  describe "iceberg_type/1" do
    test "maps Elixir types to Iceberg types" do
      assert Iceberg.Schema.iceberg_type(:string) == "string"
      assert Iceberg.Schema.iceberg_type(:long) == "long"
      assert Iceberg.Schema.iceberg_type(:int) == "int"
      assert Iceberg.Schema.iceberg_type(:integer) == "int"
      assert Iceberg.Schema.iceberg_type(:double) == "double"
      assert Iceberg.Schema.iceberg_type(:float) == "float"
      assert Iceberg.Schema.iceberg_type(:boolean) == "boolean"
      assert Iceberg.Schema.iceberg_type(:timestamp) == "timestamp"
      assert Iceberg.Schema.iceberg_type(:date) == "date"
      assert Iceberg.Schema.iceberg_type(:binary) == "binary"
      assert Iceberg.Schema.iceberg_type(:bytes) == "binary"
    end

    test "maps v1 primitive types: time, timestamptz, uuid" do
      assert Iceberg.Schema.iceberg_type(:time) == "time"
      assert Iceberg.Schema.iceberg_type(:timestamptz) == "timestamptz"
      assert Iceberg.Schema.iceberg_type(:uuid) == "uuid"
    end

    test "maps v1 parameterized types: decimal and fixed" do
      assert Iceberg.Schema.iceberg_type({:decimal, 10, 2}) == "decimal(10, 2)"
      assert Iceberg.Schema.iceberg_type({:decimal, 38, 9}) == "decimal(38, 9)"
      assert Iceberg.Schema.iceberg_type({:fixed, 16}) == "fixed[16]"
      assert Iceberg.Schema.iceberg_type({:fixed, 32}) == "fixed[32]"
    end

    test "maps v1 complex types: list" do
      # List of strings (required elements by default)
      result = Iceberg.Schema.iceberg_type({:list, :string}, element_id: 100)

      assert result == %{
               "type" => "list",
               "element-id" => 100,
               "element" => "string",
               "element-required" => true
             }

      # List of optional integers
      result = Iceberg.Schema.iceberg_type({:list, :int}, element_id: 101, required: false)

      assert result == %{
               "type" => "list",
               "element-id" => 101,
               "element" => "int",
               "element-required" => false
             }
    end

    test "maps v1 complex types: map" do
      # Map with string keys and int values (required values by default)
      result = Iceberg.Schema.iceberg_type({:map, :string, :int}, key_id: 100, value_id: 101)

      assert result == %{
               "type" => "map",
               "key-id" => 100,
               "key" => "string",
               "value-id" => 101,
               "value" => "int",
               "value-required" => true
             }

      # Map with optional values
      result =
        Iceberg.Schema.iceberg_type({:map, :string, :long},
          key_id: 102,
          value_id: 103,
          value_required: false
        )

      assert result == %{
               "type" => "map",
               "key-id" => 102,
               "key" => "string",
               "value-id" => 103,
               "value" => "long",
               "value-required" => false
             }
    end

    test "maps v1 complex types: struct" do
      # Struct with nested fields
      result =
        Iceberg.Schema.iceberg_type(
          {:struct,
           [
             {1, "field1", :string, true},
             {2, "field2", :int, false}
           ]}
        )

      assert result == %{
               "type" => "struct",
               "fields" => [
                 %{"id" => 1, "name" => "field1", "type" => "string", "required" => true},
                 %{"id" => 2, "name" => "field2", "type" => "int", "required" => false}
               ]
             }
    end
  end
end
