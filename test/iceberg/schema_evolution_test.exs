defmodule Iceberg.SchemaEvolutionTest do
  use ExUnit.Case, async: true

  alias Iceberg.SchemaEvolution

  describe "add_column/3" do
    test "adds a new column with auto-generated field ID" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      {:ok, updated_schema} =
        SchemaEvolution.add_column(schema, %{name: "email", type: "string", required: false})

      assert length(updated_schema["fields"]) == 2

      new_field = Enum.at(updated_schema["fields"], 1)
      assert new_field["id"] == 2
      assert new_field["name"] == "email"
      assert new_field["type"] == "string"
      assert new_field["required"] == false
    end

    test "adds column with documentation" do
      schema = %{"schema-id" => 0, "fields" => []}

      {:ok, updated_schema} =
        SchemaEvolution.add_column(schema, %{
          name: "count",
          type: "long",
          doc: "Number of items"
        })

      new_field = hd(updated_schema["fields"])
      assert new_field["doc"] == "Number of items"
    end

    test "generates sequential field IDs" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "field1", "type" => "string"},
          %{"id" => 5, "name" => "field2", "type" => "string"}
        ]
      }

      {:ok, updated_schema} =
        SchemaEvolution.add_column(schema, %{name: "field3", type: "string"})

      new_field = Enum.at(updated_schema["fields"], 2)
      assert new_field["id"] == 6
    end

    test "starts field IDs at 1 for empty schema" do
      schema = %{"fields" => []}

      {:ok, updated_schema} =
        SchemaEvolution.add_column(schema, %{name: "first", type: "string"})

      assert hd(updated_schema["fields"])["id"] == 1
    end

    test "rejects duplicate column name" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "email", "type" => "string", "required" => false}
        ]
      }

      assert {:error, message} =
               SchemaEvolution.add_column(schema, %{name: "email", type: "long"})

      assert message =~ "Column 'email' already exists"
    end

    test "rejects adding required column to non-empty table in strict mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:error, message} =
               SchemaEvolution.add_column(
                 schema,
                 %{name: "required_field", type: "string", required: true},
                 table_empty?: false
               )

      assert message =~ "Cannot add required column"
    end

    test "allows adding required column to non-empty table in permissive mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:ok, _updated_schema, warnings} =
               SchemaEvolution.add_column(
                 schema,
                 %{name: "required_field", type: "string", required: true},
                 mode: :permissive,
                 table_empty?: false
               )

      assert warnings =~ "Adding required column"
    end

    test "allows adding required column to empty table" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:ok, updated_schema} =
               SchemaEvolution.add_column(
                 schema,
                 %{name: "required_field", type: "string", required: true},
                 table_empty?: true
               )

      new_field = Enum.at(updated_schema["fields"], 1)
      assert new_field["required"] == true
    end

    test "accepts both atom and string keys in field_spec" do
      schema = %{"fields" => []}

      # Atom keys
      {:ok, schema1} =
        SchemaEvolution.add_column(schema, %{name: "field1", type: "string", required: false})

      # String keys (with table_empty: true since we're adding a required field)
      {:ok, schema2} =
        SchemaEvolution.add_column(
          schema1,
          %{
            "name" => "field2",
            "type" => "long",
            "required" => true
          },
          table_empty?: true
        )

      assert length(schema2["fields"]) == 2
    end

    test "skips validation with force option" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      # This would fail in strict mode but passes with force
      assert {:ok, _updated_schema} =
               SchemaEvolution.add_column(
                 schema,
                 %{name: "required_field", type: "string", required: true},
                 force: true,
                 table_empty?: false
               )
    end
  end

  describe "drop_column/3" do
    test "removes a column from the schema" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "temp", "type" => "string", "required" => false}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.drop_column(schema, "temp")

      assert length(updated_schema["fields"]) == 1
      assert hd(updated_schema["fields"])["name"] == "id"
    end

    test "rejects dropping required column in strict mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:error, message} = SchemaEvolution.drop_column(schema, "id")
      assert message =~ "Cannot drop required column"
    end

    test "allows dropping required column in permissive mode with warning" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:ok, updated_schema, warnings} =
               SchemaEvolution.drop_column(schema, "id", mode: :permissive)

      assert length(updated_schema["fields"]) == 0
      assert warnings =~ "Dropping required column"
    end

    test "rejects dropping non-existent column" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:error, message} = SchemaEvolution.drop_column(schema, "nonexistent")
      assert message =~ "Column 'nonexistent' does not exist"
    end

    test "allows dropping optional column in strict mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "optional", "type" => "string", "required" => false}
        ]
      }

      assert {:ok, updated_schema} = SchemaEvolution.drop_column(schema, "optional")
      assert length(updated_schema["fields"]) == 1
    end
  end

  describe "rename_column/4" do
    test "renames a column preserving field ID and type" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "old_name", "type" => "string", "required" => false}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.rename_column(schema, "old_name", "new_name")

      field = hd(updated_schema["fields"])
      assert field["id"] == 1
      assert field["name"] == "new_name"
      assert field["type"] == "string"
    end

    test "rejects renaming to existing column name" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "field1", "type" => "string"},
          %{"id" => 2, "name" => "field2", "type" => "long"}
        ]
      }

      assert {:error, message} = SchemaEvolution.rename_column(schema, "field1", "field2")
      assert message =~ "Column 'field2' already exists"
    end

    test "rejects renaming non-existent column" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "field1", "type" => "string"}
        ]
      }

      assert {:error, message} = SchemaEvolution.rename_column(schema, "nonexistent", "new_name")
      assert message =~ "Column 'nonexistent' does not exist"
    end
  end

  describe "update_column_type/4" do
    test "allows safe type promotion int -> long" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "count", "type" => "int", "required" => false}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.update_column_type(schema, "count", "long")

      field = hd(updated_schema["fields"])
      assert field["type"] == "long"
      assert field["id"] == 1
      assert field["name"] == "count"
    end

    test "allows safe type promotion float -> double" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "value", "type" => "float", "required" => false}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.update_column_type(schema, "value", "double")

      assert hd(updated_schema["fields"])["type"] == "double"
    end

    test "rejects unsafe type change in strict mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "count", "type" => "long", "required" => false}
        ]
      }

      assert {:error, message} = SchemaEvolution.update_column_type(schema, "count", "int")
      assert message =~ "is not a safe promotion"
    end

    test "allows unsafe type change in permissive mode with warning" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "count", "type" => "long", "required" => false}
        ]
      }

      assert {:ok, updated_schema, warnings} =
               SchemaEvolution.update_column_type(schema, "count", "int", mode: :permissive)

      assert hd(updated_schema["fields"])["type"] == "int"
      assert warnings =~ "may cause data loss"
    end

    test "allows changing to same type (no-op)" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.update_column_type(schema, "name", "string")

      assert hd(updated_schema["fields"])["type"] == "string"
    end

    test "rejects updating non-existent column" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "field1", "type" => "string"}
        ]
      }

      assert {:error, message} =
               SchemaEvolution.update_column_type(schema, "nonexistent", "long")

      assert message =~ "Column 'nonexistent' does not exist"
    end
  end

  describe "make_column_required/3" do
    test "makes an optional column required" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "email", "type" => "string", "required" => false}
        ]
      }

      {:ok, updated_schema} =
        SchemaEvolution.make_column_required(schema, "email", mode: :permissive)

      assert hd(updated_schema["fields"])["required"] == true
    end

    test "handles column without explicit required field (defaults to false)" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "email", "type" => "string"}
        ]
      }

      {:ok, updated_schema} =
        SchemaEvolution.make_column_required(schema, "email", mode: :permissive)

      assert hd(updated_schema["fields"])["required"] == true
    end

    test "no-op if column is already required" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.make_column_required(schema, "id")

      assert hd(updated_schema["fields"])["required"] == true
    end

    test "rejects making non-existent column required" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "field1", "type" => "string"}
        ]
      }

      assert {:error, message} = SchemaEvolution.make_column_required(schema, "nonexistent")
      assert message =~ "Column 'nonexistent' does not exist"
    end
  end

  describe "make_column_optional/3" do
    test "makes a required column optional in permissive mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "temp", "type" => "string", "required" => true}
        ]
      }

      {:ok, updated_schema, warnings} =
        SchemaEvolution.make_column_optional(schema, "temp", mode: :permissive)

      assert hd(updated_schema["fields"])["required"] == false
      assert warnings =~ "Changing required column to optional"
    end

    test "rejects making required column optional in strict mode" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:error, message} = SchemaEvolution.make_column_optional(schema, "id")
      assert message =~ "Cannot change required column to optional"
    end

    test "no-op if column is already optional" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "temp", "type" => "string", "required" => false}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.make_column_optional(schema, "temp")

      assert hd(updated_schema["fields"])["required"] == false
    end

    test "allows with force option" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      {:ok, updated_schema} = SchemaEvolution.make_column_optional(schema, "id", force: true)

      assert hd(updated_schema["fields"])["required"] == false
    end
  end

  describe "integration: complex schema evolution" do
    test "multiple operations on the same schema" do
      # Start with a simple schema
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "count", "type" => "int", "required" => false},
          %{"id" => 3, "name" => "old_name", "type" => "string", "required" => false}
        ]
      }

      # Add a new column
      {:ok, schema} =
        SchemaEvolution.add_column(schema, %{name: "email", type: "string", required: false})

      assert length(schema["fields"]) == 4

      # Rename a column
      {:ok, schema} = SchemaEvolution.rename_column(schema, "old_name", "new_name")
      assert Enum.any?(schema["fields"], fn f -> f["name"] == "new_name" end)

      # Update column type (safe promotion)
      {:ok, schema} = SchemaEvolution.update_column_type(schema, "count", "long")
      count_field = Enum.find(schema["fields"], fn f -> f["name"] == "count" end)
      assert count_field["type"] == "long"

      # Make column required
      {:ok, schema} = SchemaEvolution.make_column_required(schema, "email", mode: :permissive)
      email_field = Enum.find(schema["fields"], fn f -> f["name"] == "email" end)
      assert email_field["required"] == true

      # Drop a column
      {:ok, schema} = SchemaEvolution.drop_column(schema, "new_name")
      assert length(schema["fields"]) == 3

      # Verify final state
      field_names = Enum.map(schema["fields"], & &1["name"])
      assert "id" in field_names
      assert "count" in field_names
      assert "email" in field_names
      refute "old_name" in field_names
      refute "new_name" in field_names
    end

    test "field IDs are never reused after dropping columns" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "field1", "type" => "string"},
          %{"id" => 2, "name" => "field2", "type" => "string"},
          %{"id" => 3, "name" => "field3", "type" => "string"}
        ]
      }

      # Drop field with ID 2
      {:ok, schema} = SchemaEvolution.drop_column(schema, "field2", force: true)

      # Add a new field - should get ID 4, not 2
      {:ok, schema} = SchemaEvolution.add_column(schema, %{name: "field4", type: "string"})

      new_field = Enum.find(schema["fields"], fn f -> f["name"] == "field4" end)
      assert new_field["id"] == 4

      # Verify ID 2 was not reused
      field_ids = Enum.map(schema["fields"], & &1["id"])
      assert field_ids == [1, 3, 4]
    end
  end
end
