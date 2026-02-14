defmodule Iceberg.SchemaEvolution do
  @moduledoc """
  Schema evolution operations for Iceberg tables.

  Provides imperative API for evolving table schemas with validation.
  Each operation creates a new schema version while preserving field IDs
  and maintaining backwards compatibility.

  ## Operations

  - `add_column/3` - Add a new column to the schema
  - `drop_column/3` - Drop a column from the schema
  - `rename_column/4` - Rename a column
  - `update_column_type/4` - Change a column's type (safe promotions only)
  - `make_column_required/3` - Make an optional column required
  - `make_column_optional/3` - Make a required column optional

  ## Validation Modes

  - `:strict` (default) - Only safe, backwards-compatible changes
  - `:permissive` - Allow more changes with warnings
  - `:none` / `force: true` - Skip validation (user responsibility)

  ## Example

      schema = %{
        "schema-id" => 0,
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "long", "required" => true}
        ]
      }

      # Add a new column
      {:ok, schema} = SchemaEvolution.add_column(schema, %{
        name: "email",
        type: "string",
        required: false
      })

      # Rename a column
      {:ok, schema} = SchemaEvolution.rename_column(schema, "email", "email_address")

      # Update column type (safe promotion)
      {:ok, schema} = SchemaEvolution.update_column_type(schema, "count", "long")
  """

  alias Iceberg.SchemaValidator

  @type schema :: map()
  @type field_spec :: map()
  @type validation_mode :: :strict | :permissive | :none

  @doc """
  Adds a new column to the schema.

  ## Parameters
  - schema: Current schema map
  - field_spec: Field specification with:
    - `:name` (required) - Field name
    - `:type` (required) - Field type
    - `:required` (optional) - Whether field is required (default: false)
    - `:doc` (optional) - Documentation string
  - opts: Options
    - `:mode` - Validation mode (default: :strict)
    - `:table_empty?` - Whether table has data (default: false)
    - `:historical_schemas` - Previous schemas for ID reuse checking

  ## Returns
  - `{:ok, updated_schema}` - Schema with new column added
  - `{:ok, updated_schema, warnings}` - Success with warnings (permissive mode)
  - `{:error, reason}` - Validation or operation failed

  ## Examples

      iex> schema = %{"schema-id" => 0, "fields" => []}
      iex> {:ok, schema} = SchemaEvolution.add_column(schema, %{name: "id", type: "long"})
      iex> length(schema["fields"])
      1
  """
  @spec add_column(schema(), field_spec(), keyword()) ::
          {:ok, schema()} | {:ok, schema(), String.t()} | {:error, term()}
  def add_column(schema, field_spec, opts \\ []) do
    mode = get_validation_mode(opts)
    name = to_string(field_spec[:name] || field_spec["name"])
    type = field_spec[:type] || field_spec["type"]
    required = field_spec[:required] || field_spec["required"] || false
    doc = field_spec[:doc] || field_spec["doc"]

    # Generate next field ID (use provided next_field_id or calculate from schema and historical schemas)
    next_id = get_next_field_id(schema, opts)

    new_field = %{
      "id" => next_id,
      "name" => name,
      "type" => type,
      "required" => required
    }

    new_field = if doc, do: Map.put(new_field, "doc", doc), else: new_field

    # Validate the addition
    case SchemaValidator.validate_add_column(schema, new_field, mode, opts) do
      :ok ->
        updated_schema = append_field(schema, new_field)
        {:ok, updated_schema}

      {:ok, warnings} ->
        updated_schema = append_field(schema, new_field)
        {:ok, updated_schema, warnings}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Drops a column from the schema.

  Note: Field IDs are never reused. The dropped field ID is permanently reserved.

  ## Parameters
  - schema: Current schema map
  - field_name: Name of field to drop
  - opts: Options
    - `:mode` - Validation mode (default: :strict)

  ## Returns
  - `{:ok, updated_schema}` - Schema with column removed
  - `{:ok, updated_schema, warnings}` - Success with warnings (permissive mode)
  - `{:error, reason}` - Validation or operation failed

  ## Examples

      iex> schema = %{"fields" => [%{"id" => 1, "name" => "old_field", "required" => false}]}
      iex> {:ok, schema} = SchemaEvolution.drop_column(schema, "old_field")
      iex> length(schema["fields"])
      0
  """
  @spec drop_column(schema(), String.t(), keyword()) ::
          {:ok, schema()} | {:ok, schema(), String.t()} | {:error, term()}
  def drop_column(schema, field_name, opts \\ []) do
    mode = get_validation_mode(opts)

    case SchemaValidator.validate_drop_column(schema, field_name, mode) do
      :ok ->
        updated_schema = remove_field(schema, field_name)
        {:ok, updated_schema}

      {:ok, warnings} ->
        updated_schema = remove_field(schema, field_name)
        {:ok, updated_schema, warnings}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Renames a column in the schema.

  Field ID and type are preserved. Only the name changes.

  ## Parameters
  - schema: Current schema map
  - old_name: Current field name
  - new_name: New field name
  - opts: Options
    - `:mode` - Validation mode (default: :strict)

  ## Returns
  - `{:ok, updated_schema}` - Schema with column renamed
  - `{:error, reason}` - Validation or operation failed

  ## Examples

      iex> schema = %{"fields" => [%{"id" => 1, "name" => "old_name", "type" => "string"}]}
      iex> {:ok, schema} = SchemaEvolution.rename_column(schema, "old_name", "new_name")
      iex> hd(schema["fields"])["name"]
      "new_name"
  """
  @spec rename_column(schema(), String.t(), String.t(), keyword()) ::
          {:ok, schema()} | {:error, term()}
  def rename_column(schema, old_name, new_name, opts \\ []) do
    mode = get_validation_mode(opts)

    case SchemaValidator.validate_rename_column(schema, old_name, new_name, mode) do
      :ok ->
        updated_schema = update_field_name(schema, old_name, new_name)
        {:ok, updated_schema}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Updates a column's type (safe promotions only).

  ## Safe Type Promotions
  - int → long
  - float → double

  ## Parameters
  - schema: Current schema map
  - field_name: Name of field to update
  - new_type: New type for the field
  - opts: Options
    - `:mode` - Validation mode (default: :strict)

  ## Returns
  - `{:ok, updated_schema}` - Schema with column type updated
  - `{:ok, updated_schema, warnings}` - Success with warnings (permissive mode)
  - `{:error, reason}` - Validation or operation failed

  ## Examples

      iex> schema = %{"fields" => [%{"id" => 1, "name" => "count", "type" => "int"}]}
      iex> {:ok, schema} = SchemaEvolution.update_column_type(schema, "count", "long")
      iex> hd(schema["fields"])["type"]
      "long"
  """
  @spec update_column_type(schema(), String.t(), term(), keyword()) ::
          {:ok, schema()} | {:ok, schema(), String.t()} | {:error, term()}
  def update_column_type(schema, field_name, new_type, opts \\ []) do
    mode = get_validation_mode(opts)

    with {:ok, field} <- find_field(schema, field_name),
         old_type = field["type"],
         validation_result <-
           SchemaValidator.validate_type_promotion(old_type, new_type, mode) do
      case validation_result do
        :ok ->
          updated_schema = update_field_type(schema, field_name, new_type)
          {:ok, updated_schema}

        {:ok, warnings} ->
          updated_schema = update_field_type(schema, field_name, new_type)
          {:ok, updated_schema, warnings}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Makes an optional column required.

  Warning: This may cause issues if existing data contains null values.
  Use with caution, preferably on empty tables or after data validation.

  ## Parameters
  - schema: Current schema map
  - field_name: Name of field to make required
  - opts: Options
    - `:mode` - Validation mode (default: :strict)

  ## Returns
  - `{:ok, updated_schema}` - Schema with column made required
  - `{:error, reason}` - Validation or operation failed

  ## Examples

      iex> schema = %{"fields" => [%{"id" => 1, "name" => "email", "required" => false}]}
      iex> {:ok, schema} = SchemaEvolution.make_column_required(schema, "email", mode: :permissive)
      iex> hd(schema["fields"])["required"]
      true
  """
  @spec make_column_required(schema(), String.t(), keyword()) ::
          {:ok, schema()} | {:error, term()}
  def make_column_required(schema, field_name, opts \\ []) do
    mode = get_validation_mode(opts)

    with {:ok, field} <- find_field(schema, field_name),
         old_required = field["required"] || false,
         result <- SchemaValidator.validate_required_promotion(old_required, true, mode) do
      case result do
        :ok ->
          updated_schema = update_field_required(schema, field_name, true)
          {:ok, updated_schema}

        {:ok, _warnings} ->
          updated_schema = update_field_required(schema, field_name, true)
          {:ok, updated_schema}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Makes a required column optional.

  This is generally not recommended as it weakens data quality guarantees.
  Only allowed in permissive mode.

  ## Parameters
  - schema: Current schema map
  - field_name: Name of field to make optional
  - opts: Options
    - `:mode` - Validation mode (default: :strict, must use :permissive or :none)

  ## Returns
  - `{:ok, updated_schema}` - Schema with column made optional
  - `{:ok, updated_schema, warnings}` - Success with warnings (permissive mode)
  - `{:error, reason}` - Validation failed (strict mode)

  ## Examples

      iex> schema = %{"fields" => [%{"id" => 1, "name" => "temp", "required" => true}]}
      iex> {:ok, schema, _warnings} = SchemaEvolution.make_column_optional(schema, "temp", mode: :permissive)
      iex> hd(schema["fields"])["required"]
      false
  """
  @spec make_column_optional(schema(), String.t(), keyword()) ::
          {:ok, schema()} | {:ok, schema(), String.t()} | {:error, term()}
  def make_column_optional(schema, field_name, opts \\ []) do
    mode = get_validation_mode(opts)

    with {:ok, field} <- find_field(schema, field_name),
         old_required = field["required"] || false,
         result <- SchemaValidator.validate_required_promotion(old_required, false, mode) do
      case result do
        :ok ->
          updated_schema = update_field_required(schema, field_name, false)
          {:ok, updated_schema}

        {:ok, warnings} ->
          updated_schema = update_field_required(schema, field_name, false)
          {:ok, updated_schema, warnings}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  ## Private Helper Functions

  defp get_validation_mode(opts) do
    cond do
      Keyword.get(opts, :force, false) -> :none
      Keyword.has_key?(opts, :mode) -> Keyword.get(opts, :mode)
      true -> :strict
    end
  end

  defp get_next_field_id(schema, opts) do
    # If next_field_id is provided explicitly (e.g., from metadata's last-column-id), use it
    if Keyword.has_key?(opts, :next_field_id) do
      Keyword.get(opts, :next_field_id)
    else
      # Otherwise calculate from current schema and historical schemas
      historical_schemas = Keyword.get(opts, :historical_schemas, [])

      all_field_ids =
        Enum.flat_map([schema | historical_schemas], fn s ->
          get_fields(s) |> Enum.map(& &1["id"])
        end)

      if Enum.empty?(all_field_ids) do
        1
      else
        Enum.max(all_field_ids) + 1
      end
    end
  end

  defp get_fields(%{"fields" => fields}) when is_list(fields), do: fields
  defp get_fields(_), do: []

  defp append_field(schema, new_field) do
    fields = get_fields(schema)
    Map.put(schema, "fields", fields ++ [new_field])
  end

  defp remove_field(schema, field_name) do
    fields = get_fields(schema)
    updated_fields = Enum.reject(fields, fn f -> f["name"] == field_name end)
    Map.put(schema, "fields", updated_fields)
  end

  defp update_field_name(schema, old_name, new_name) do
    fields = get_fields(schema)

    updated_fields =
      Enum.map(fields, fn field ->
        if field["name"] == old_name do
          Map.put(field, "name", new_name)
        else
          field
        end
      end)

    Map.put(schema, "fields", updated_fields)
  end

  defp update_field_type(schema, field_name, new_type) do
    fields = get_fields(schema)

    updated_fields =
      Enum.map(fields, fn field ->
        if field["name"] == field_name do
          Map.put(field, "type", new_type)
        else
          field
        end
      end)

    Map.put(schema, "fields", updated_fields)
  end

  defp update_field_required(schema, field_name, required) do
    fields = get_fields(schema)

    updated_fields =
      Enum.map(fields, fn field ->
        if field["name"] == field_name do
          Map.put(field, "required", required)
        else
          field
        end
      end)

    Map.put(schema, "fields", updated_fields)
  end

  defp find_field(schema, field_name) do
    fields = get_fields(schema)

    case Enum.find(fields, fn f -> f["name"] == field_name end) do
      nil -> {:error, "Column '#{field_name}' does not exist"}
      field -> {:ok, field}
    end
  end
end
