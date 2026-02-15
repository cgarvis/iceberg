defmodule Iceberg.SingleValue do
  @moduledoc """
  Encodes Iceberg values according to the single-value serialization specification.

  Single values are used in lower_bounds and upper_bounds in manifest files.
  Each type has a specific binary encoding format defined by the Iceberg spec.

  ## References
  - https://iceberg.apache.org/spec/#appendix-d-single-value-serialization
  """

  @doc """
  Encodes a single value to its binary representation according to Iceberg spec.

  ## Parameters
    - value: The value to encode
    - type: The Iceberg type as a string (e.g., "int", "long", "string")

  ## Returns
    - Binary data or nil if value is nil

  ## Examples

      iex> Iceberg.SingleValue.encode(42, "int")
      <<42, 0, 0, 0>>

      iex> Iceberg.SingleValue.encode("hello", "string")
      <<"hello">>

      iex> Iceberg.SingleValue.encode(nil, "int")
      nil
  """
  def encode(nil, _type), do: nil

  # Boolean: 0x00 for false, 0x01 for true
  def encode(false, "boolean"), do: <<0x00>>
  def encode(true, "boolean"), do: <<0x01>>

  # Int: 4 bytes in little-endian
  def encode(value, "int") when is_integer(value) do
    <<value::integer-signed-little-32>>
  end

  # Long: 8 bytes in little-endian
  def encode(value, "long") when is_integer(value) do
    <<value::integer-signed-little-64>>
  end

  # Float: 4 bytes IEEE 754 in little-endian
  def encode(value, "float") when is_float(value) or is_integer(value) do
    <<value::float-little-32>>
  end

  # Double: 8 bytes IEEE 754 in little-endian
  def encode(value, "double") when is_float(value) or is_integer(value) do
    <<value::float-little-64>>
  end

  # Date: days from 1970-01-01 as 4-byte int
  def encode(%Date{} = value, "date") do
    days = Date.diff(value, ~D[1970-01-01])
    <<days::integer-signed-little-32>>
  end

  # Timestamp: microseconds from epoch as 8-byte long
  def encode(%DateTime{} = value, "timestamp") do
    micros = DateTime.to_unix(value, :microsecond)
    <<micros::integer-signed-little-64>>
  end

  # Timestamptz: same as timestamp
  def encode(%DateTime{} = value, "timestamptz") do
    micros = DateTime.to_unix(value, :microsecond)
    <<micros::integer-signed-little-64>>
  end

  # String: UTF-8 bytes (without length prefix)
  def encode(value, "string") when is_binary(value) do
    value
  end

  # Binary: raw bytes (without length prefix)
  def encode(value, "binary") when is_binary(value) do
    value
  end

  # UUID: 16 bytes in big-endian (RFC 4122)
  def encode(value, "uuid") when is_binary(value) do
    # Parse UUID string format: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    value
    |> String.replace("-", "")
    |> String.upcase()
    |> Base.decode16!()
  end

  # Fixed[L]: L bytes
  def encode(value, "fixed[" <> _) when is_binary(value) do
    value
  end

  # Decimal: unscaled value as big-endian bytes
  # For now, just pass through - full decimal support requires more work
  def encode(value, "decimal(" <> _) when is_binary(value) do
    value
  end

  # Time: microseconds from midnight as 8-byte long
  def encode(%Time{} = value, "time") do
    micros = Time.diff(value, ~T[00:00:00.000000], :microsecond)
    <<micros::integer-signed-little-64>>
  end

  # Fallback for unknown types
  def encode(value, _type) when is_binary(value), do: value
  def encode(_value, _type), do: nil

  @doc """
  Encodes a map of column_id => value using the schema to determine types.

  ## Parameters
    - bounds: Map of column_id (integer) => value
    - schema: Table schema with field definitions

  ## Returns
    - Map of column_id => encoded binary or nil

  ## Examples

      iex> schema = %{"fields" => [%{"id" => 1, "type" => "int"}]}
      iex> Iceberg.SingleValue.encode_bounds_map(%{1 => 42}, schema)
      %{1 => <<42, 0, 0, 0>>}
  """
  def encode_bounds_map(nil, _schema), do: nil
  def encode_bounds_map(bounds, _schema) when bounds == %{}, do: %{}

  def encode_bounds_map(bounds, schema) when is_map(bounds) do
    # Build a map of field_id => type
    type_map = build_type_map(schema)

    bounds
    |> Enum.map(fn {col_id, value} ->
      # Normalize col_id to integer (might be string)
      normalized_id = normalize_column_id(col_id)
      type = Map.get(type_map, normalized_id)

      if type && value != nil do
        {normalized_id, encode(value, type)}
      else
        nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.into(%{})
  end

  # Normalize column ID to integer (handles both int and string keys)
  defp normalize_column_id(id) when is_integer(id), do: id
  defp normalize_column_id(id) when is_binary(id), do: String.to_integer(id)
  defp normalize_column_id(_), do: nil

  # Private functions

  defp build_type_map(schema) do
    fields = get_fields(schema)

    Enum.reduce(fields, %{}, fn field, acc ->
      id = get_field_id(field)
      type = get_field_type(field)
      Map.put(acc, id, type)
    end)
  end

  defp get_fields(%{"fields" => fields}), do: fields
  defp get_fields(%{fields: fields}), do: fields
  defp get_fields(_), do: []

  defp get_field_id(%{"id" => id}), do: id
  defp get_field_id(%{id: id}), do: id
  defp get_field_id(_), do: nil

  defp get_field_type(%{"type" => type}), do: type
  defp get_field_type(%{type: type}), do: type
  defp get_field_type(_), do: nil
end
