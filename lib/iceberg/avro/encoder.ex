defmodule Iceberg.Avro.Encoder do
  import Bitwise

  @moduledoc """
  Encodes Elixir data structures into Avro Object Container Files.

  Implements Apache Avro 1.11 specification for container files with support
  for primitive types (long, int, string, bytes, boolean) and complex types
  (record, array, map, union).

  ## Avro Object Container File Format

  ```
  [Magic Bytes: 0x4F 0x62 0x6A 0x01]
  [Header Block]
    - avro.schema: JSON schema as Avro bytes
    - avro.codec: Compression codec (null, deflate, snappy)
    - Sync marker: 16 random bytes
  [Data Block 1]
    - Object count (long)
    - Byte count (long)
    - Serialized objects
    - Sync marker (16 bytes)
  [Data Block 2...]
  ```

  ## Examples

      schema = %{
        "type" => "record",
        "name" => "User",
        "fields" => [
          %{"name" => "id", "type" => "long"},
          %{"name" => "name", "type" => "string"}
        ]
      }

      records = [
        %{"id" => 1, "name" => "Alice"},
        %{"id" => 2, "name" => "Bob"}
      ]

      binary = Iceberg.Avro.Encoder.encode(records, schema)
      File.write!("users.avro", binary)
  """

  @magic_bytes <<0x4F, 0x62, 0x6A, 0x01>>

  @doc """
  Encodes a list of records into an Avro Object Container File.

  ## Parameters
    - records: List of maps matching the schema
    - schema: Avro schema as a map
    - opts: Options
      - `:codec` - Compression codec (`:null`, `:deflate`). Default: `:null`
      - `:block_size` - Target block size in records. Default: 100

  ## Returns
    Binary data ready to write to file
  """
  @spec encode(list(map()), map(), keyword()) :: binary()
  def encode(records, schema, opts \\ []) do
    codec = Keyword.get(opts, :codec, :null)
    block_size = Keyword.get(opts, :block_size, 100)
    extra_metadata = Keyword.get(opts, :metadata, %{})

    # Generate sync marker (16 random bytes)
    sync_marker = :crypto.strong_rand_bytes(16)

    # Build file
    header = encode_header(schema, codec, sync_marker, extra_metadata)
    data_blocks = encode_data_blocks(records, schema, sync_marker, block_size)

    IO.iodata_to_binary([@magic_bytes, header, data_blocks])
  end

  ## Private Functions

  # Encodes the Avro container file header
  defp encode_header(schema, codec, sync_marker, extra_metadata) do
    # Header is a map with metadata
    # Include standard Avro metadata plus any Iceberg-specific metadata
    metadata =
      Map.merge(extra_metadata, %{
        "avro.schema" => JSON.encode!(schema),
        "avro.codec" => codec_name(codec)
      })

    # Encode metadata as map: long count, then key-value pairs
    metadata_count = encode_long(map_size(metadata))

    metadata_entries =
      Enum.map(metadata, fn {key, value} ->
        [encode_string(key), encode_bytes(value)]
      end)

    # End of map (0 count)
    metadata_end = encode_long(0)

    IO.iodata_to_binary([metadata_count, metadata_entries, metadata_end, sync_marker])
  end

  # Encodes all data blocks
  defp encode_data_blocks(records, schema, sync_marker, block_size) do
    records
    |> Enum.chunk_every(block_size)
    |> Enum.map(&encode_data_block(&1, schema, sync_marker))
  end

  # Encodes a single data block
  defp encode_data_block(records, schema, sync_marker) do
    encoded_records =
      Enum.map(records, &encode_record(&1, schema))
      |> IO.iodata_to_binary()

    object_count = encode_long(length(records))
    byte_count = encode_long(byte_size(encoded_records))

    IO.iodata_to_binary([object_count, byte_count, encoded_records, sync_marker])
  end

  # Encodes a single record according to schema
  defp encode_record(record, %{"type" => "record", "fields" => fields}) do
    Enum.map(fields, fn field ->
      field_name = field["name"]
      field_type = field["type"]
      field_value = Map.get(record, field_name)

      encode_value(field_value, field_type)
    end)
  end

  # Encodes a value according to its type
  defp encode_value(nil, ["null" | _rest]), do: encode_long(0)

  defp encode_value(value, ["null", actual_type]),
    do: [encode_long(1), encode_value(value, actual_type)]

  defp encode_value(value, [_null, actual_type | _rest]) when not is_nil(value),
    do: [encode_long(1), encode_value(value, actual_type)]

  defp encode_value(value, "long") when is_integer(value), do: encode_long(value)
  defp encode_value(nil, "long"), do: encode_long(0)
  defp encode_value(value, "int") when is_integer(value), do: encode_int(value)
  defp encode_value(nil, "int"), do: encode_int(0)
  defp encode_value(value, "string"), do: encode_string(value)
  defp encode_value(value, "bytes") when is_binary(value), do: encode_bytes(value)
  defp encode_value(nil, "bytes"), do: encode_bytes("")
  defp encode_value(true, "boolean"), do: <<1>>
  defp encode_value(false, "boolean"), do: <<0>>

  # Record type
  defp encode_value(value, %{"type" => "record"} = schema) when is_map(value) do
    encode_record(value, schema)
  end

  # Array type with logicalType="map" (Iceberg spec - for maps with non-string keys)
  # Schema: { "type": "array", "logicalType": "map", "items": { "type": "record", ... } }
  # Value: Elixir map that needs to be converted to array of {key, value} records
  defp encode_value(map, %{"type" => "array", "logicalType" => "map", "items" => record_schema})
       when is_map(map) do
    if map_size(map) == 0 do
      encode_long(0)
    else
      count = encode_long(map_size(map))

      # Convert map entries to records with "key" and "value" fields
      items =
        Enum.map(map, fn {k, v} ->
          # Ensure key is an integer if it came in as a string
          key_value = if is_binary(k), do: String.to_integer(k), else: k
          record = %{"key" => key_value, "value" => v}
          encode_value(record, record_schema)
        end)

      block_end = encode_long(0)
      [count, items, block_end]
    end
  end

  # Array type (standard)
  defp encode_value(values, %{"type" => "array", "items" => item_type}) when is_list(values) do
    if Enum.empty?(values) do
      encode_long(0)
    else
      count = encode_long(length(values))
      items = Enum.map(values, &encode_value(&1, item_type))
      block_end = encode_long(0)
      [count, items, block_end]
    end
  end

  # Map type (standard Avro - keys are always strings)
  defp encode_value(map, %{"type" => "map", "values" => value_type}) when is_map(map) do
    if map_size(map) == 0 do
      encode_long(0)
    else
      count = encode_long(map_size(map))

      entries =
        Enum.map(map, fn {key, value} ->
          [encode_string(to_string(key)), encode_value(value, value_type)]
        end)

      block_end = encode_long(0)
      [count, entries, block_end]
    end
  end

  # Null value
  defp encode_value(nil, _type), do: <<>>

  ## Primitive Encodings

  @doc false
  def encode_long(value) when is_integer(value) do
    # Zigzag encoding: convert signed to unsigned
    zigzag = Bitwise.bxor(value <<< 1, value >>> 63)
    encode_varint(zigzag)
  end

  @doc false
  def encode_int(value) when is_integer(value) do
    # Zigzag encoding for 32-bit int
    zigzag = Bitwise.bxor(value <<< 1, value >>> 31)
    encode_varint(zigzag)
  end

  @doc false
  def encode_string(value) when is_binary(value) do
    size = byte_size(value)
    [encode_long(size), value]
  end

  # Handle nil string values by encoding as empty string
  def encode_string(nil), do: encode_string("")

  @doc false
  def encode_bytes(value) when is_binary(value) do
    size = byte_size(value)
    [encode_long(size), value]
  end

  # Variable-length integer encoding (7 bits per byte, MSB = continuation)
  defp encode_varint(value) when value < 128 do
    <<value>>
  end

  defp encode_varint(value) do
    byte = Bitwise.band(value, 0x7F) ||| 0x80
    rest = value >>> 7
    [<<byte>>, encode_varint(rest)]
  end

  # Codec name for header
  defp codec_name(:null), do: "null"
  defp codec_name(:deflate), do: "deflate"
  defp codec_name(:snappy), do: "snappy"
end
