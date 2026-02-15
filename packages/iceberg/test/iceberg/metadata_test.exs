defmodule Iceberg.MetadataTest do
  use ExUnit.Case

  alias Iceberg.Metadata
  alias Iceberg.Storage.Memory

  @opts [storage: Memory, base_url: "memory://test"]

  setup do
    Memory.clear()
    :ok
  end

  describe "create_initial/4" do
    test "produces all required v2 metadata fields" do
      schema = %{
        "type" => "record",
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "required" => true, "type" => "string"},
          %{"id" => 2, "name" => "name", "required" => false, "type" => "string"}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}

      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)

      # All 17 required v2 metadata fields
      assert metadata["format-version"] == 2
      assert is_binary(metadata["table-uuid"])
      assert is_binary(metadata["location"])
      assert metadata["last-sequence-number"] == 0
      assert is_integer(metadata["last-updated-ms"])
      assert metadata["last-column-id"] == 2
      assert is_list(metadata["schemas"])
      assert metadata["current-schema-id"] == 0
      assert is_list(metadata["partition-specs"])
      assert metadata["default-spec-id"] == 0
      assert is_integer(metadata["last-partition-id"])
      assert is_map(metadata["properties"])
      assert metadata["current-snapshot-id"] == -1
      assert metadata["snapshots"] == []
      assert metadata["snapshot-log"] == []
      assert metadata["metadata-log"] == []
      assert is_list(metadata["sort-orders"])
      assert metadata["default-sort-order-id"] == 0
    end

    test "format-version is 2" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      assert metadata["format-version"] == 2
    end

    test "current-snapshot-id is -1" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      assert metadata["current-snapshot-id"] == -1
    end

    test "snapshots is empty list" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      assert metadata["snapshots"] == []
    end

    test "UUID is valid v4 format" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      uuid = metadata["table-uuid"]

      assert Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               uuid
             )
    end

    test "includes name mapping in properties" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id"},
          %{"id" => 2, "name" => "name"}
        ]
      }

      {:ok, metadata} =
        Metadata.create_initial("test/table", schema, %{"fields" => []}, @opts)

      name_mapping = metadata["properties"]["schema.name-mapping.default"]
      assert is_binary(name_mapping)
      decoded = JSON.decode!(name_mapping)
      assert length(decoded) == 2
      assert Enum.at(decoded, 0)["field-id"] == 1
      assert Enum.at(decoded, 0)["names"] == ["id"]
    end
  end

  describe "save/3 and load/2" do
    test "round-trips through storage" do
      schema = %{
        "type" => "record",
        "schema-id" => 0,
        "fields" => [%{"id" => 1, "name" => "id", "required" => true, "type" => "string"}]
      }

      {:ok, metadata} =
        Metadata.create_initial("test/table", schema, %{"spec-id" => 0, "fields" => []}, @opts)

      # Save sets version to last-sequence-number (0), but the save function uses it
      # Initial save will use version 0
      :ok = Metadata.save("test/table", metadata, @opts)

      # Load should return the same metadata
      {:ok, loaded} = Metadata.load("test/table", @opts)

      assert loaded["format-version"] == metadata["format-version"]
      assert loaded["table-uuid"] == metadata["table-uuid"]
      assert loaded["current-snapshot-id"] == metadata["current-snapshot-id"]
    end

    test "version-hint.text contains correct version number" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      :ok = Metadata.save("test/table", metadata, @opts)

      {:ok, hint} = Memory.download("test/table/metadata/version-hint.text")
      assert String.trim(hint) == "0"
    end
  end

  describe "add_snapshot/2" do
    test "increments sequence number" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => System.system_time(:millisecond),
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert updated["last-sequence-number"] == 1
    end

    test "updates current-snapshot-id" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => System.system_time(:millisecond),
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert updated["current-snapshot-id"] == 12_345
    end

    test "appends to snapshot-log" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => 1000,
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert length(updated["snapshot-log"]) == 1
      [log_entry] = updated["snapshot-log"]
      assert log_entry["snapshot-id"] == 12_345
      assert log_entry["timestamp-ms"] == 1000
    end

    test "appends snapshot to snapshots list" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => 1000,
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)

      assert length(updated["snapshots"]) == 1
      assert List.first(updated["snapshots"])["snapshot-id"] == 12_345
    end
  end

  describe "exists?/2" do
    test "returns false for non-existent table" do
      refute Metadata.exists?("nonexistent/table", @opts)
    end

    test "returns true for existing table" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      :ok = Metadata.save("test/table", metadata, @opts)

      assert Metadata.exists?("test/table", @opts)
    end
  end

  describe "get_last_processed_file/2" do
    test "returns nil for table with no snapshots" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      :ok = Metadata.save("test/table", metadata, @opts)

      assert {:ok, nil} = Metadata.get_last_processed_file("test/table", @opts)
    end

    test "extracts source-file from summary" do
      {:ok, metadata} =
        Metadata.create_initial("test/table", %{"fields" => []}, %{"fields" => []}, @opts)

      snapshot = %{
        "snapshot-id" => 12_345,
        "timestamp-ms" => 1000,
        "manifest-list" => "memory://test/table/metadata/snap-12_345.avro",
        "summary" => %{"operation" => "append", "source-file" => "sources/data.jsonl"},
        "schema-id" => 0
      }

      {:ok, updated} = Metadata.add_snapshot(metadata, snapshot)
      :ok = Metadata.save("test/table", updated, @opts)

      assert {:ok, "sources/data.jsonl"} =
               Metadata.get_last_processed_file("test/table", @opts)
    end

    test "returns nil for non-existent table" do
      assert {:ok, nil} = Metadata.get_last_processed_file("nonexistent", @opts)
    end
  end

  describe "evolve_schema/3" do
    test "adds a new column and updates metadata correctly" do
      # Create initial table
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Evolve schema to add a column
      {:ok, updated_metadata} =
        Metadata.evolve_schema(
          "test/table",
          fn current_schema, ctx ->
            Iceberg.SchemaEvolution.add_column(
              current_schema,
              %{
                name: "email",
                type: "string",
                required: false
              },
              ctx
            )
          end,
          @opts
        )

      # Verify new schema was created
      assert updated_metadata["current-schema-id"] == 1
      assert length(updated_metadata["schemas"]) == 2

      # Verify old schema is preserved
      old_schema = Enum.at(updated_metadata["schemas"], 0)
      assert old_schema["schema-id"] == 0
      assert length(old_schema["fields"]) == 1

      # Verify new schema has both fields
      new_schema = Enum.at(updated_metadata["schemas"], 1)
      assert new_schema["schema-id"] == 1
      assert length(new_schema["fields"]) == 2

      new_field = Enum.find(new_schema["fields"], fn f -> f["name"] == "email" end)
      assert new_field["id"] == 2
      assert new_field["type"] == "string"

      # Verify last-column-id was updated
      assert updated_metadata["last-column-id"] == 2

      # Verify last-updated-ms was updated (should be >= since operations might happen in same ms)
      assert updated_metadata["last-updated-ms"] >= metadata["last-updated-ms"]

      # Verify name mapping was updated
      name_mapping_json = updated_metadata["properties"]["schema.name-mapping.default"]
      name_mapping = JSON.decode!(name_mapping_json)
      assert length(name_mapping) == 2
      assert Enum.any?(name_mapping, fn m -> m["field-id"] == 2 && "email" in m["names"] end)
    end

    test "drops a column and preserves field IDs" do
      # Create initial table with multiple fields
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "temp", "type" => "string", "required" => false},
          %{"id" => 3, "name" => "data", "type" => "string", "required" => false}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Drop the temp column
      {:ok, updated_metadata} =
        Metadata.evolve_schema(
          "test/table",
          fn current_schema, _ctx ->
            Iceberg.SchemaEvolution.drop_column(current_schema, "temp")
          end,
          @opts
        )

      # Verify new schema was created
      new_schema = Metadata.get_current_schema(updated_metadata)
      assert new_schema["schema-id"] == 1
      assert length(new_schema["fields"]) == 2

      # Verify remaining fields
      field_names = Enum.map(new_schema["fields"], & &1["name"])
      assert "id" in field_names
      assert "data" in field_names
      refute "temp" in field_names

      # Verify last-column-id remains at highest ever used (3, not 2)
      assert updated_metadata["last-column-id"] == 3
    end

    test "renames a column preserving field ID" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "old_name", "type" => "string", "required" => false}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Rename column
      {:ok, updated_metadata} =
        Metadata.evolve_schema(
          "test/table",
          fn current_schema, _ctx ->
            Iceberg.SchemaEvolution.rename_column(current_schema, "old_name", "new_name")
          end,
          @opts
        )

      new_schema = Metadata.get_current_schema(updated_metadata)
      field = hd(new_schema["fields"])
      assert field["id"] == 1
      assert field["name"] == "new_name"
      assert field["type"] == "string"
    end

    test "updates column type with safe promotion" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "count", "type" => "int", "required" => false}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Promote int to long
      {:ok, updated_metadata} =
        Metadata.evolve_schema(
          "test/table",
          fn current_schema, _ctx ->
            Iceberg.SchemaEvolution.update_column_type(current_schema, "count", "long")
          end,
          @opts
        )

      new_schema = Metadata.get_current_schema(updated_metadata)
      field = hd(new_schema["fields"])
      assert field["type"] == "long"
    end

    test "returns warnings in permissive mode" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Add required field to non-empty table (would fail in strict mode)
      {:ok, updated_metadata, warnings} =
        Metadata.evolve_schema(
          "test/table",
          fn current_schema, ctx ->
            Iceberg.SchemaEvolution.add_column(
              current_schema,
              %{name: "required_field", type: "string", required: true},
              Keyword.merge(ctx, mode: :permissive, table_empty?: false)
            )
          end,
          @opts
        )

      assert is_binary(warnings)
      assert warnings =~ "Adding required column"
      assert updated_metadata["current-schema-id"] == 1
    end

    test "returns error when evolution fails validation" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Try to drop required column in strict mode
      assert {:error, message} =
               Metadata.evolve_schema(
                 "test/table",
                 fn current_schema, _ctx ->
                   Iceberg.SchemaEvolution.drop_column(current_schema, "id")
                 end,
                 @opts
               )

      assert message =~ "Cannot drop required column"
    end

    test "multiple schema evolutions create sequential schema IDs" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # First evolution
      {:ok, metadata1} =
        Metadata.evolve_schema(
          "test/table",
          fn s, ctx ->
            Iceberg.SchemaEvolution.add_column(s, %{name: "field1", type: "string"}, ctx)
          end,
          @opts
        )

      :ok = Metadata.save("test/table", metadata1, @opts)
      assert metadata1["current-schema-id"] == 1

      # Second evolution
      {:ok, metadata2} =
        Metadata.evolve_schema(
          "test/table",
          fn s, ctx ->
            Iceberg.SchemaEvolution.add_column(s, %{name: "field2", type: "string"}, ctx)
          end,
          @opts
        )

      :ok = Metadata.save("test/table", metadata2, @opts)
      assert metadata2["current-schema-id"] == 2

      # Verify all three schemas are preserved
      assert length(metadata2["schemas"]) == 3
      schema_ids = Enum.map(metadata2["schemas"], & &1["schema-id"])
      assert schema_ids == [0, 1, 2]
    end

    test "preserves historical schemas for field ID reuse prevention" do
      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true},
          %{"id" => 2, "name" => "temp", "type" => "string", "required" => false}
        ]
      }

      partition_spec = %{"spec-id" => 0, "fields" => []}
      {:ok, metadata} = Metadata.create_initial("test/table", schema, partition_spec, @opts)
      :ok = Metadata.save("test/table", metadata, @opts)

      # Drop field with ID 2
      {:ok, metadata1} =
        Metadata.evolve_schema(
          "test/table",
          fn s, ctx ->
            Iceberg.SchemaEvolution.drop_column(s, "temp", Keyword.put(ctx, :force, true))
          end,
          @opts
        )

      :ok = Metadata.save("test/table", metadata1, @opts)

      # Add a new field - should get ID 3, not reuse ID 2
      {:ok, metadata2} =
        Metadata.evolve_schema(
          "test/table",
          fn s, ctx ->
            Iceberg.SchemaEvolution.add_column(s, %{name: "new_field", type: "string"}, ctx)
          end,
          @opts
        )

      current_schema = Metadata.get_current_schema(metadata2)
      new_field = Enum.find(current_schema["fields"], fn f -> f["name"] == "new_field" end)
      assert new_field["id"] == 3

      # Verify field ID 2 was not reused
      field_ids = Enum.map(current_schema["fields"], & &1["id"])
      assert field_ids == [1, 3]
    end
  end

  describe "get_current_schema/1" do
    test "returns the schema matching current-schema-id" do
      metadata = %{
        "current-schema-id" => 1,
        "schemas" => [
          %{"schema-id" => 0, "fields" => [%{"id" => 1, "name" => "old"}]},
          %{"schema-id" => 1, "fields" => [%{"id" => 1, "name" => "new"}]}
        ]
      }

      schema = Metadata.get_current_schema(metadata)
      assert schema["schema-id"] == 1
      assert hd(schema["fields"])["name"] == "new"
    end

    test "returns first schema when current-schema-id is 0" do
      metadata = %{
        "current-schema-id" => 0,
        "schemas" => [
          %{"schema-id" => 0, "fields" => [%{"id" => 1, "name" => "first"}]}
        ]
      }

      schema = Metadata.get_current_schema(metadata)
      assert schema["schema-id"] == 0
      assert hd(schema["fields"])["name"] == "first"
    end
  end
end
