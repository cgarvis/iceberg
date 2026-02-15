defmodule Iceberg.SchemaValidatorTest do
  use ExUnit.Case, async: true

  alias Iceberg.SchemaValidator

  describe "validate_add_column/4" do
    test "allows adding new column with unique field ID" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      new_field = %{"id" => 3, "name" => "age", "type" => "int", "required" => false}

      assert :ok = SchemaValidator.validate_add_column(schema, new_field, :strict, _opts = [])
    end

    test "rejects adding column with duplicate field ID in strict mode" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      new_field = %{"id" => 2, "name" => "age", "type" => "int", "required" => false}

      assert {:error, message} =
               SchemaValidator.validate_add_column(schema, new_field, :strict, _opts = [])

      assert message =~ "Field ID 2 is already in use"
    end

    test "rejects adding column with duplicate name in strict mode" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      new_field = %{"id" => 3, "name" => "name", "type" => "int", "required" => false}

      assert {:error, message} =
               SchemaValidator.validate_add_column(schema, new_field, :strict, _opts = [])

      assert message =~ "Column 'name' already exists"
    end

    test "rejects adding required column to non-empty table in strict mode" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      new_field = %{"id" => 2, "name" => "name", "type" => "string", "required" => true}

      assert {:error, message} =
               SchemaValidator.validate_add_column(schema, new_field, :strict,
                 table_empty?: false
               )

      assert message =~ "Cannot add required column 'name' to non-empty table"
    end

    test "allows adding required column to empty table" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      new_field = %{"id" => 2, "name" => "name", "type" => "string", "required" => true}

      assert :ok =
               SchemaValidator.validate_add_column(schema, new_field, :strict, table_empty?: true)
    end

    test "allows adding column in permissive mode with warnings" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      new_field = %{"id" => 2, "name" => "name", "type" => "string", "required" => true}

      assert {:ok, warnings} =
               SchemaValidator.validate_add_column(schema, new_field, :permissive,
                 table_empty?: false
               )

      assert warnings =~ "Adding required column 'name' to non-empty table"
    end

    test "skips validation when mode is :none" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      # Even with duplicate ID, should pass in :none mode
      new_field = %{"id" => 1, "name" => "duplicate", "type" => "string", "required" => true}

      assert :ok = SchemaValidator.validate_add_column(schema, new_field, :none, _opts = [])
    end
  end

  describe "validate_drop_column/3" do
    test "allows dropping optional column in strict mode" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert :ok = SchemaValidator.validate_drop_column(schema, "name", :strict)
    end

    test "rejects dropping required column in strict mode" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert {:error, message} = SchemaValidator.validate_drop_column(schema, "id", :strict)
      assert message =~ "Cannot drop required column 'id'"
    end

    test "rejects dropping non-existent column" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:error, message} =
               SchemaValidator.validate_drop_column(schema, "nonexistent", :strict)

      assert message =~ "Column 'nonexistent' does not exist"
    end

    test "allows dropping required column in permissive mode with warning" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert {:ok, warnings} = SchemaValidator.validate_drop_column(schema, "id", :permissive)
      assert warnings =~ "Dropping required column 'id'"
    end

    test "skips validation when mode is :none" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert :ok = SchemaValidator.validate_drop_column(schema, "nonexistent", :none)
    end
  end

  describe "validate_rename_column/4" do
    test "allows renaming column to unique name" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert :ok = SchemaValidator.validate_rename_column(schema, "name", "full_name", :strict)
    end

    test "rejects renaming to existing column name" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert {:error, message} =
               SchemaValidator.validate_rename_column(schema, "name", "id", :strict)

      assert message =~ "Column 'id' already exists"
    end

    test "rejects renaming non-existent column" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert {:error, message} =
               SchemaValidator.validate_rename_column(schema, "nonexistent", "new_name", :strict)

      assert message =~ "Column 'nonexistent' does not exist"
    end

    test "skips validation when mode is :none" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert :ok = SchemaValidator.validate_rename_column(schema, "id", "id", :none)
    end
  end

  describe "validate_type_promotion/3" do
    test "allows safe type promotions" do
      # int → long
      assert :ok = SchemaValidator.validate_type_promotion("int", "long", :strict)

      # float → double
      assert :ok = SchemaValidator.validate_type_promotion("float", "double", :strict)
    end

    test "rejects unsafe type promotions in strict mode" do
      # long → int (narrowing)
      assert {:error, message} = SchemaValidator.validate_type_promotion("long", "int", :strict)
      assert message =~ "is not a safe promotion"

      # double → float (narrowing)
      assert {:error, message} =
               SchemaValidator.validate_type_promotion("double", "float", :strict)

      assert message =~ "is not a safe promotion"

      # string → int (incompatible)
      assert {:error, message} = SchemaValidator.validate_type_promotion("string", "int", :strict)
      assert message =~ "is not a safe promotion"
    end

    test "allows same type" do
      assert :ok = SchemaValidator.validate_type_promotion("string", "string", :strict)
      assert :ok = SchemaValidator.validate_type_promotion("long", "long", :strict)
    end

    test "allows unsafe promotions in permissive mode with warning" do
      assert {:ok, warnings} = SchemaValidator.validate_type_promotion("long", "int", :permissive)
      assert warnings =~ "may cause data loss"
    end

    test "skips validation when mode is :none" do
      assert :ok = SchemaValidator.validate_type_promotion("string", "int", :none)
    end
  end

  describe "validate_required_promotion/3" do
    test "allows optional → required promotion" do
      assert :ok = SchemaValidator.validate_required_promotion(false, true, :strict)
    end

    test "allows keeping same required status" do
      assert :ok = SchemaValidator.validate_required_promotion(true, true, :strict)
      assert :ok = SchemaValidator.validate_required_promotion(false, false, :strict)
    end

    test "rejects required → optional demotion in strict mode" do
      assert {:error, message} = SchemaValidator.validate_required_promotion(true, false, :strict)
      assert message =~ "Cannot change required column to optional"
    end

    test "allows required → optional in permissive mode with warning" do
      assert {:ok, warnings} =
               SchemaValidator.validate_required_promotion(true, false, :permissive)

      assert warnings =~ "Changing required column to optional"
    end

    test "skips validation when mode is :none" do
      assert :ok = SchemaValidator.validate_required_promotion(true, false, :none)
    end
  end

  describe "validate_field_id_not_reused/3" do
    test "allows field ID that was never used" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert :ok = SchemaValidator.validate_field_id_not_reused(schema, 3, _opts = [])
    end

    test "rejects field ID currently in use" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => false}
        ]
      }

      assert {:error, message} =
               SchemaValidator.validate_field_id_not_reused(schema, 2, _opts = [])

      assert message =~ "Field ID 2 is already in use by column 'name'"
    end

    test "rejects field ID used in historical schemas" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      # Field ID 2 was used in a previous schema version
      historical_schemas = [
        %{
          "schema-id" => 0,
          "fields" => [
            %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
            %{"id" => 2, "name" => "deleted_field", "type" => "string", "required" => false}
          ]
        }
      ]

      assert {:error, message} =
               SchemaValidator.validate_field_id_not_reused(schema, 2,
                 historical_schemas: historical_schemas
               )

      assert message =~ "was previously used and cannot be reused"
    end

    test "allows reuse when no historical schemas provided" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      assert :ok = SchemaValidator.validate_field_id_not_reused(schema, 2, _opts = [])
    end
  end

  describe "integration: multiple validations" do
    test "combining multiple validation failures" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "name", "type" => "string", "required" => true}
        ]
      }

      # Try to add a field with duplicate ID AND duplicate name
      new_field = %{"id" => 1, "name" => "name", "type" => "int", "required" => false}

      assert {:error, message} =
               SchemaValidator.validate_add_column(schema, new_field, :strict, _opts = [])

      # Should catch the first error (duplicate ID or name)
      assert message =~ ~r/(Field ID 1 is already in use|Column 'name' already exists)/
    end

    test "permissive mode accumulates warnings" do
      schema = %{
        "schema-id" => 1,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      new_field = %{"id" => 2, "name" => "email", "type" => "string", "required" => true}

      assert {:ok, warnings} =
               SchemaValidator.validate_add_column(schema, new_field, :permissive,
                 table_empty?: false
               )

      assert warnings =~ "Adding required column 'email' to non-empty table"
    end
  end
end
