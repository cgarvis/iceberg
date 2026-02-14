defmodule Iceberg.Schema do
  @moduledoc """
  DSL for defining Iceberg table schemas (Ecto.Schema-like).

  Provides a compile-time schema definition with type safety and validation.

  ## Example

      defmodule MyApp.Schemas.Events do
        use Iceberg.Schema

        schema "canonical/events" do
          field :id, :string, required: true
          field :timestamp, :timestamp, required: true
          field :data, :string
          field :count, :long

          partition day(:timestamp)
        end
      end

      # Usage:
      Iceberg.Table.create(MyApp.Schemas.Events)
      Iceberg.Table.insert_overwrite(conn, MyApp.Schemas.Events, "SELECT * FROM staging")

  ## Field Types

  ### Primitive Types (V1)
  - `:string` - Arbitrary-length character sequences
  - `:long` - 64-bit signed integers
  - `:int` - 32-bit signed integers
  - `:double` - 64-bit IEEE 754 floating point
  - `:float` - 32-bit IEEE 754 floating point
  - `:boolean` - True or false
  - `:timestamp` - Timestamp with microsecond precision (no timezone)
  - `:timestamptz` - Timestamp with microsecond precision (with timezone)
  - `:date` - Date without time
  - `:time` - Time of day without date
  - `:uuid` - Universally unique identifier
  - `:binary` - Arbitrary-length byte array
  - `{:decimal, precision, scale}` - Fixed-point decimal (e.g., `{:decimal, 10, 2}`)
  - `{:fixed, length}` - Fixed-length byte array (e.g., `{:fixed, 16}`)

  ### Complex Types (V1)
  - `{:list, element_type}` - List of elements of the given type
  - `{:map, key_type, value_type}` - Map with keys and values of given types
  - `{:struct, fields}` - Nested struct with fields list: `[{id, name, type, required}, ...]`

  ## Partition Transforms

  - `identity(field)` - Identity transform
  - `day(field)` - Day partition
  - `month(field)` - Month partition
  - `year(field)` - Year partition
  """

  defmacro __using__(_opts) do
    quote do
      import Iceberg.Schema, only: [schema: 2, field: 2, field: 3, partition: 1]
      Module.register_attribute(__MODULE__, :iceberg_fields, accumulate: true)
      Module.register_attribute(__MODULE__, :iceberg_partition_spec, accumulate: false)
      Module.register_attribute(__MODULE__, :iceberg_table_path, accumulate: false)
      @before_compile Iceberg.Schema
    end
  end

  @doc """
  Defines the table schema.
  """
  defmacro schema(table_path, do: block) do
    quote do
      @iceberg_table_path unquote(table_path)
      unquote(block)
    end
  end

  @doc """
  Defines a field in the schema.

  ## Options
    - `required: true` - Field cannot be null (default: false)
    - `doc: "..."` - Documentation string
  """
  defmacro field(name, type, opts \\ []) do
    quote bind_quoted: [name: name, type: type, opts: opts] do
      required = Keyword.get(opts, :required, false)
      doc = Keyword.get(opts, :doc)
      @iceberg_fields {name, type, required, doc}
    end
  end

  @doc """
  Defines partition specification.

  ## Examples
      partition day(:timestamp)
      partition month(:date_col)
      partition year(:date_col)
      partition hour(:timestamp)
      partition identity(:category)
      partition bucket(:user_id, 10)
      partition truncate(:name, 5)
  """
  # Simple transforms (no parameters)
  defmacro partition({transform, _meta, [field]}) do
    quote bind_quoted: [transform: transform, field: field] do
      @iceberg_partition_spec {transform, field, nil}
    end
  end

  # Parameterized transforms (bucket[N], truncate[W])
  defmacro partition({transform, _meta, [field, param]}) do
    quote bind_quoted: [transform: transform, field: field, param: param] do
      @iceberg_partition_spec {transform, field, param}
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def __table_path__, do: @iceberg_table_path

      def __schema__ do
        fields = @iceberg_fields |> Enum.reverse()

        iceberg_fields =
          Enum.with_index(fields, fn {name, type, required, _doc}, idx ->
            %{
              "id" => idx + 1,
              "name" => to_string(name),
              "required" => required,
              "type" => Iceberg.Schema.iceberg_type(type)
            }
          end)

        %{
          "type" => "struct",
          "schema-id" => 0,
          "fields" => iceberg_fields
        }
      end

      def __partition_spec__ do
        Iceberg.Schema.build_partition_spec(
          @iceberg_partition_spec,
          @iceberg_fields |> Enum.reverse()
        )
      end

      def __fields__ do
        @iceberg_fields
        |> Enum.reverse()
        |> Enum.map(fn {name, type, required, _doc} ->
          {name, type, required}
        end)
      end

      def __partition_field__ do
        Iceberg.Schema.get_partition_field(@iceberg_partition_spec)
      end
    end
  end

  @doc false
  def get_partition_field(nil), do: nil
  def get_partition_field({_transform, field, _param}), do: field

  @doc false
  def build_partition_spec(nil, _fields) do
    %{"spec-id" => 0, "fields" => []}
  end

  def build_partition_spec({transform, field, param}, fields) do
    field_id = find_field_id(fields, field)

    if field_id do
      %{
        "spec-id" => 0,
        "fields" => [build_partition_field(transform, field, param, field_id)]
      }
    else
      raise "Partition field #{field} not found in schema"
    end
  end

  defp find_field_id(fields, field) do
    Enum.find_index(fields, fn {name, _, _, _} -> name == field end)
  end

  defp build_partition_field(transform, field, nil, field_id) do
    %{
      "name" => "#{field}_#{transform}",
      "transform" => to_string(transform),
      "source-id" => field_id + 1,
      "field-id" => 1000 + field_id
    }
  end

  defp build_partition_field(transform, field, param, field_id) do
    %{
      "name" => "#{field}_#{transform}",
      "transform" => "#{transform}[#{param}]",
      "source-id" => field_id + 1,
      "field-id" => 1000 + field_id
    }
  end

  @doc false
  # Primitive types
  def iceberg_type(:string), do: "string"
  def iceberg_type(:long), do: "long"
  def iceberg_type(:int), do: "int"
  def iceberg_type(:integer), do: "int"
  def iceberg_type(:double), do: "double"
  def iceberg_type(:float), do: "float"
  def iceberg_type(:boolean), do: "boolean"
  def iceberg_type(:timestamp), do: "timestamp"
  def iceberg_type(:date), do: "date"
  def iceberg_type(:binary), do: "binary"
  def iceberg_type(:bytes), do: "binary"
  # V1 primitive types
  def iceberg_type(:time), do: "time"
  def iceberg_type(:timestamptz), do: "timestamptz"
  def iceberg_type(:uuid), do: "uuid"
  # V1 parameterized types
  def iceberg_type({:decimal, precision, scale}), do: "decimal(#{precision}, #{scale})"
  def iceberg_type({:fixed, length}), do: "fixed[#{length}]"

  # V1 complex types - struct
  def iceberg_type({:struct, fields}) when is_list(fields) do
    iceberg_fields =
      Enum.map(fields, fn {id, name, type, required} ->
        %{
          "id" => id,
          "name" => name,
          "type" => resolve_type(type),
          "required" => required
        }
      end)

    %{
      "type" => "struct",
      "fields" => iceberg_fields
    }
  end

  # V1 complex types - list (with options)
  def iceberg_type({:list, element_type}, opts) do
    element_id = Keyword.fetch!(opts, :element_id)
    element_required = Keyword.get(opts, :required, true)

    %{
      "type" => "list",
      "element-id" => element_id,
      "element" => resolve_type(element_type),
      "element-required" => element_required
    }
  end

  # V1 complex types - map (with options)
  def iceberg_type({:map, key_type, value_type}, opts) do
    key_id = Keyword.fetch!(opts, :key_id)
    value_id = Keyword.fetch!(opts, :value_id)
    value_required = Keyword.get(opts, :value_required, true)

    %{
      "type" => "map",
      "key-id" => key_id,
      "key" => resolve_type(key_type),
      "value-id" => value_id,
      "value" => resolve_type(value_type),
      "value-required" => value_required
    }
  end

  # Helper to resolve nested types
  defp resolve_type(type) when is_atom(type), do: iceberg_type(type)
  defp resolve_type(type) when is_tuple(type), do: iceberg_type(type)
end
