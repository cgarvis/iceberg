defmodule Iceberg.Test.AvroParser do
  @moduledoc """
  Minimal Avro OCF header parser for testing.

  Extracts the `avro.schema` JSON and other metadata from Avro Object
  Container File binary output. This enables spec-compliance assertions
  on manifest field IDs without needing a full Avro decoder.
  """

  import Bitwise

  @magic_bytes <<0x4F, 0x62, 0x6A, 0x01>>

  @doc """
  Parses the header of an Avro OCF binary, returning metadata map and sync marker.

  ## Returns
    `{:ok, %{"avro.schema" => ..., "avro.codec" => ...}, sync_marker}` on success
    `{:error, reason}` on failure
  """
  def parse_header(<<@magic_bytes, rest::binary>>) do
    with {:ok, metadata, rest} <- decode_map(rest) do
      # Next 16 bytes are the sync marker
      <<sync_marker::binary-size(16), _rest::binary>> = rest
      {:ok, metadata, sync_marker}
    end
  end

  def parse_header(_), do: {:error, :invalid_magic_bytes}

  @doc """
  Extracts and parses the avro.schema JSON from an Avro OCF binary.

  ## Returns
    `{:ok, schema_map}` on success
    `{:error, reason}` on failure
  """
  def parse_schema(binary) do
    with {:ok, metadata, _sync} <- parse_header(binary) do
      case Map.get(metadata, "avro.schema") do
        nil -> {:error, :no_schema}
        json -> {:ok, JSON.decode!(json)}
      end
    end
  end

  @doc """
  Extracts all header metadata from an Avro OCF binary.
  """
  def parse_metadata(binary) do
    with {:ok, metadata, _sync} <- parse_header(binary) do
      {:ok, metadata}
    end
  end

  ## Private - Avro decoding helpers

  defp decode_map(binary) do
    {count, rest} = decode_long(binary)

    if count == 0 do
      {:ok, %{}, rest}
    else
      {entries, rest} = decode_map_entries(abs(count), rest, %{})
      # Read terminating 0
      {0, rest} = decode_long(rest)
      {:ok, entries, rest}
    end
  end

  defp decode_map_entries(0, rest, acc), do: {acc, rest}

  defp decode_map_entries(count, binary, acc) do
    {key, rest} = decode_string(binary)
    {value, rest} = decode_bytes(rest)
    decode_map_entries(count - 1, rest, Map.put(acc, key, value))
  end

  defp decode_long(binary) do
    {unsigned, rest} = decode_varint(binary, 0, 0)
    # Zigzag decode
    value = Bitwise.bxor(unsigned >>> 1, -(unsigned &&& 1))
    {value, rest}
  end

  defp decode_varint(<<byte, rest::binary>>, shift, acc) do
    value = acc ||| Bitwise.band(byte, 0x7F) <<< shift

    if Bitwise.band(byte, 0x80) == 0 do
      {value, rest}
    else
      decode_varint(rest, shift + 7, value)
    end
  end

  defp decode_string(binary) do
    {length, rest} = decode_long(binary)
    <<string::binary-size(length), rest::binary>> = rest
    {string, rest}
  end

  defp decode_bytes(binary) do
    {length, rest} = decode_long(binary)
    <<bytes::binary-size(length), rest::binary>> = rest
    {bytes, rest}
  end
end
