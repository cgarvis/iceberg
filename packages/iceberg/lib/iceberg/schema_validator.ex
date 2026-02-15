defmodule Iceberg.SchemaValidator do
  @moduledoc """
  Validation rules for schema evolution operations.

  Provides validation for schema changes to ensure compatibility and prevent
  data corruption. Supports multiple validation modes:

  - `:strict` - Only allow safe, backwards-compatible changes (default)
  - `:permissive` - Allow more changes with warnings
  - `:none` - Skip validation (use with `force: true`)

  ## Safe Type Promotions

  The following type promotions are considered safe:
  - `int` → `long` (32-bit to 64-bit integer)
  - `float` → `double` (32-bit to 64-bit float)

  ## Field ID Rules

  Field IDs must never be reused. Once assigned, a field ID is permanently
  reserved even if the field is dropped.
  """

  @type validation_mode :: :strict | :permissive | :none
  @type schema :: map()

  @safe_type_promotions %{
    "int" => ["long"],
    "float" => ["double"],
    :int => [:long],
    :float => [:double]
  }

  @doc """
  Validates adding a new column to the schema.

  ## Validation Rules (Strict Mode)
  - Field ID must not be reused
  - Field name must not already exist
  - Field type must be valid
  - Cannot add required field to non-empty table

  ## Parameters
  - schema: Current schema map with "fields" key
  - new_field: Map with "id", "name", "type", and "required" keys
  - mode: Validation mode (:strict, :permissive, :none)
  - opts: Options
    - `:table_empty?` - Boolean, whether table has data (default: false)
    - `:historical_schemas` - List of previous schemas for ID reuse checking

  ## Returns
  - `:ok` - Validation passed
  - `{:ok, warnings}` - Passed with warnings (permissive mode)
  - `{:error, reason}` - Validation failed
  """
  @spec validate_add_column(schema(), map(), validation_mode(), keyword()) ::
          :ok | {:ok, String.t()} | {:error, term()}
  def validate_add_column(schema, new_field, mode, opts \\ [])

  def validate_add_column(_schema, _new_field, :none, _opts), do: :ok

  def validate_add_column(schema, new_field, mode, opts) do
    table_empty? = Keyword.get(opts, :table_empty?, false)
    field_id = new_field["id"] || new_field[:id]
    field_name = new_field["name"] || new_field[:name]
    field_type = new_field["type"] || new_field[:type]
    required = new_field["required"] || new_field[:required] || false

    with :ok <- validate_field_id_not_reused(schema, field_id, opts),
         :ok <- validate_field_name_available(schema, field_name),
         :ok <- validate_field_type(field_type) do
      if required and not table_empty? do
        case mode do
          :strict ->
            {:error,
             "Cannot add required column '#{field_name}' to non-empty table in strict mode"}

          :permissive ->
            {:ok, "Adding required column '#{field_name}' to non-empty table may cause issues"}
        end
      else
        :ok
      end
    end
  end

  @doc """
  Validates dropping a column from the schema.

  ## Validation Rules
  - Field must exist in schema
  - Strict mode: Cannot drop required fields
  - Permissive mode: Can drop any field (with warning)

  ## Parameters
  - schema: Current schema map with "fields" key
  - field_name: Name of field to drop
  - mode: Validation mode (:strict, :permissive, :none)

  ## Returns
  - `:ok` - Validation passed
  - `{:ok, warnings}` - Passed with warnings (permissive mode)
  - `{:error, reason}` - Validation failed
  """
  @spec validate_drop_column(schema(), String.t(), validation_mode()) ::
          :ok | {:ok, String.t()} | {:error, term()}
  def validate_drop_column(_schema, _field_name, :none), do: :ok

  def validate_drop_column(schema, field_name, mode) do
    with {:ok, field} <- find_field(schema, field_name),
         :ok <- validate_can_drop_field(field, mode) do
      if mode == :permissive and field["required"] do
        {:ok, "Dropping required column '#{field_name}' may break existing queries"}
      else
        :ok
      end
    end
  end

  @doc """
  Validates renaming a column.

  ## Validation Rules
  - Old field name must exist
  - New field name must not exist
  - Field type and ID preserved

  ## Parameters
  - schema: Current schema map with "fields" key
  - old_name: Current field name
  - new_name: New field name
  - mode: Validation mode (:strict, :permissive, :none)

  ## Returns
  - `:ok` - Validation passed
  - `{:error, reason}` - Validation failed
  """
  @spec validate_rename_column(schema(), String.t(), String.t(), validation_mode()) ::
          :ok | {:error, term()}
  def validate_rename_column(_schema, _old_name, _new_name, :none), do: :ok

  def validate_rename_column(schema, old_name, new_name, _mode) do
    with {:ok, _field} <- find_field(schema, old_name),
         :ok <- validate_field_name_available(schema, new_name) do
      :ok
    end
  end

  @doc """
  Validates a type promotion (type change).

  ## Safe Promotions
  - int → long
  - float → double

  ## Parameters
  - old_type: Current field type
  - new_type: New field type
  - mode: Validation mode (:strict, :permissive, :none)

  ## Returns
  - `:ok` - Safe promotion
  - `{:ok, warnings}` - Unsafe promotion in permissive mode
  - `{:error, reason}` - Invalid promotion
  """
  @spec validate_type_promotion(term(), term(), validation_mode()) ::
          :ok | {:ok, String.t()} | {:error, term()}
  def validate_type_promotion(_old_type, _new_type, :none), do: :ok

  def validate_type_promotion(old_type, new_type, mode) do
    old_type_normalized = normalize_type(old_type)
    new_type_normalized = normalize_type(new_type)

    cond do
      old_type_normalized == new_type_normalized ->
        :ok

      is_safe_promotion?(old_type_normalized, new_type_normalized) ->
        :ok

      mode == :permissive ->
        {:ok, "Type change from #{inspect(old_type)} to #{inspect(new_type)} may cause data loss"}

      mode == :strict ->
        {:error,
         "Type change from #{inspect(old_type)} to #{inspect(new_type)} is not a safe promotion. Only safe promotions allowed: int→long, float→double"}

      true ->
        :ok
    end
  end

  @doc """
  Validates changing the required status of a field.

  ## Validation Rules
  - Optional → Required: Allowed (may need data validation)
  - Required → Optional: Not allowed in strict mode
  - Same → Same: Always allowed

  ## Parameters
  - old_required: Current required status (boolean)
  - new_required: New required status (boolean)
  - mode: Validation mode (:strict, :permissive, :none)

  ## Returns
  - `:ok` - Validation passed
  - `{:ok, warnings}` - Allowed with warnings
  - `{:error, reason}` - Validation failed
  """
  @spec validate_required_promotion(boolean(), boolean(), validation_mode()) ::
          :ok | {:ok, String.t()} | {:error, term()}
  def validate_required_promotion(_old_required, _new_required, :none), do: :ok

  def validate_required_promotion(old_required, new_required, mode) do
    cond do
      old_required == new_required ->
        :ok

      # Optional → Required
      not old_required and new_required ->
        :ok

      # Required → Optional (demotion)
      old_required and not new_required and mode == :strict ->
        {:error, "Cannot change required column to optional in strict mode"}

      old_required and not new_required and mode == :permissive ->
        {:ok, "Changing required column to optional may affect data quality expectations"}

      true ->
        :ok
    end
  end

  @doc """
  Validates that field IDs are not being reused.

  This is a critical validation that prevents field ID reuse, which would
  violate the Iceberg specification.

  ## Parameters
  - schema: Current schema map with "fields" key
  - new_field_id: Field ID being assigned
  - opts: Options
    - `:historical_schemas` - List of previous schemas to check

  ## Returns
  - `:ok` - Field ID is not in use
  - `{:error, reason}` - Field ID is already used
  """
  @spec validate_field_id_not_reused(schema(), integer(), keyword()) ::
          :ok | {:error, term()}
  def validate_field_id_not_reused(schema, new_field_id, opts \\ []) do
    fields = get_fields(schema)
    historical_schemas = Keyword.get(opts, :historical_schemas, [])

    # Check current schema
    case Enum.find(fields, fn f -> f["id"] == new_field_id end) do
      nil ->
        # Check historical schemas
        check_historical_schemas(historical_schemas, new_field_id)

      field ->
        {:error, "Field ID #{new_field_id} is already in use by column '#{field["name"]}'"}
    end
  end

  defp check_historical_schemas([], _field_id), do: :ok

  defp check_historical_schemas(historical_schemas, field_id) do
    all_historical_ids =
      Enum.flat_map(historical_schemas, fn schema ->
        get_fields(schema) |> Enum.map(& &1["id"])
      end)

    if field_id in all_historical_ids do
      {:error, "Field ID #{field_id} was previously used and cannot be reused per Iceberg spec"}
    else
      :ok
    end
  end

  ## Private Functions

  defp validate_field_name_available(schema, field_name) do
    case find_field(schema, field_name) do
      {:ok, _field} ->
        {:error, "Column '#{field_name}' already exists"}

      {:error, _message} ->
        :ok
    end
  end

  defp validate_field_type(field_type) do
    if valid_iceberg_type?(field_type) do
      :ok
    else
      {:error, "Type #{inspect(field_type)} is not a valid Iceberg type"}
    end
  end

  defp validate_can_drop_field(field, :strict) do
    if field["required"] do
      {:error, "Cannot drop required column '#{field["name"]}' in strict mode"}
    else
      :ok
    end
  end

  defp validate_can_drop_field(_field, _mode), do: :ok

  defp find_field(schema, field_name) do
    fields = get_fields(schema)

    case Enum.find(fields, fn f -> f["name"] == field_name end) do
      nil -> {:error, "Column '#{field_name}' does not exist"}
      field -> {:ok, field}
    end
  end

  defp get_fields(%{"fields" => fields}) when is_list(fields), do: fields
  defp get_fields(_), do: []

  defp is_safe_promotion?(old_type, new_type) do
    allowed = Map.get(@safe_type_promotions, old_type, [])
    new_type in allowed
  end

  defp normalize_type(type) when is_atom(type), do: type
  defp normalize_type(type) when is_binary(type), do: type
  defp normalize_type({:decimal, _, _}), do: :decimal
  defp normalize_type({:fixed, _}), do: :fixed
  defp normalize_type({:list, _}), do: :list
  defp normalize_type({:map, _, _}), do: :map
  defp normalize_type({:struct, _}), do: :struct
  defp normalize_type(type), do: type

  # Validate Iceberg types (both atom and string formats)
  defp valid_iceberg_type?(:string), do: true
  defp valid_iceberg_type?("string"), do: true
  defp valid_iceberg_type?(:long), do: true
  defp valid_iceberg_type?("long"), do: true
  defp valid_iceberg_type?(:int), do: true
  defp valid_iceberg_type?("int"), do: true
  defp valid_iceberg_type?(:double), do: true
  defp valid_iceberg_type?("double"), do: true
  defp valid_iceberg_type?(:float), do: true
  defp valid_iceberg_type?("float"), do: true
  defp valid_iceberg_type?(:boolean), do: true
  defp valid_iceberg_type?("boolean"), do: true
  defp valid_iceberg_type?(:timestamp), do: true
  defp valid_iceberg_type?("timestamp"), do: true
  defp valid_iceberg_type?(:timestamptz), do: true
  defp valid_iceberg_type?("timestamptz"), do: true
  defp valid_iceberg_type?(:date), do: true
  defp valid_iceberg_type?("date"), do: true
  defp valid_iceberg_type?(:time), do: true
  defp valid_iceberg_type?("time"), do: true
  defp valid_iceberg_type?(:uuid), do: true
  defp valid_iceberg_type?("uuid"), do: true
  defp valid_iceberg_type?(:binary), do: true
  defp valid_iceberg_type?("binary"), do: true
  defp valid_iceberg_type?({:decimal, _precision, _scale}), do: true
  defp valid_iceberg_type?({:fixed, _length}), do: true
  defp valid_iceberg_type?({:list, element_type}), do: valid_iceberg_type?(element_type)

  defp valid_iceberg_type?({:map, key_type, value_type}),
    do: valid_iceberg_type?(key_type) and valid_iceberg_type?(value_type)

  defp valid_iceberg_type?({:struct, _fields}), do: true
  defp valid_iceberg_type?(_), do: false
end
