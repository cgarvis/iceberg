defmodule Iceberg.Avro.EncoderTest do
  use ExUnit.Case, async: true

  alias Iceberg.Avro.Encoder

  describe "encode/3" do
    test "encodes simple record with primitives" do
      schema = %{
        "type" => "record",
        "name" => "Test",
        "fields" => [
          %{"name" => "id", "type" => "long"},
          %{"name" => "name", "type" => "string"}
        ]
      }

      records = [%{"id" => 1, "name" => "foo"}]
      binary = Encoder.encode(records, schema)

      # Verify magic bytes
      assert <<0x4F, 0x62, 0x6A, 0x01, _rest::binary>> = binary
    end

    test "encodes multiple records" do
      schema = %{
        "type" => "record",
        "name" => "User",
        "fields" => [
          %{"name" => "id", "type" => "long"},
          %{"name" => "active", "type" => "boolean"}
        ]
      }

      records = [
        %{"id" => 1, "active" => true},
        %{"id" => 2, "active" => false},
        %{"id" => 3, "active" => true}
      ]

      binary = Encoder.encode(records, schema)

      # Should have magic bytes + header + at least one data block
      assert byte_size(binary) > 100
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end

    test "encodes record with nullable fields" do
      schema = %{
        "type" => "record",
        "name" => "OptionalFields",
        "fields" => [
          %{"name" => "required_id", "type" => "long"},
          %{"name" => "optional_name", "type" => ["null", "string"]}
        ]
      }

      records = [
        %{"required_id" => 1, "optional_name" => "Alice"},
        %{"required_id" => 2, "optional_name" => nil}
      ]

      binary = Encoder.encode(records, schema)
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end

    test "encodes record with nested record" do
      schema = %{
        "type" => "record",
        "name" => "Parent",
        "fields" => [
          %{"name" => "id", "type" => "long"},
          %{
            "name" => "child",
            "type" => %{
              "type" => "record",
              "name" => "Child",
              "fields" => [
                %{"name" => "name", "type" => "string"}
              ]
            }
          }
        ]
      }

      records = [
        %{"id" => 1, "child" => %{"name" => "nested"}}
      ]

      binary = Encoder.encode(records, schema)
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end

    test "encodes record with array field" do
      schema = %{
        "type" => "record",
        "name" => "WithArray",
        "fields" => [
          %{"name" => "id", "type" => "long"},
          %{"name" => "tags", "type" => %{"type" => "array", "items" => "string"}}
        ]
      }

      records = [
        %{"id" => 1, "tags" => ["foo", "bar", "baz"]},
        %{"id" => 2, "tags" => []}
      ]

      binary = Encoder.encode(records, schema)
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end

    test "encodes record with map field" do
      schema = %{
        "type" => "record",
        "name" => "WithMap",
        "fields" => [
          %{"name" => "id", "type" => "long"},
          %{"name" => "metadata", "type" => %{"type" => "map", "values" => "long"}}
        ]
      }

      records = [
        %{"id" => 1, "metadata" => %{"count" => 42, "size" => 1024}},
        %{"id" => 2, "metadata" => %{}}
      ]

      binary = Encoder.encode(records, schema)
      assert <<0x4F, 0x62, 0x6A, 0x01, _::binary>> = binary
    end

    test "uses block size option" do
      schema = %{
        "type" => "record",
        "name" => "Test",
        "fields" => [%{"name" => "id", "type" => "long"}]
      }

      records = Enum.map(1..250, &%{"id" => &1})

      # Default block size (100) should create 3 blocks
      binary = Encoder.encode(records, schema, block_size: 100)
      assert byte_size(binary) > 500

      # Larger block size should create fewer blocks
      binary_large = Encoder.encode(records, schema, block_size: 500)
      assert byte_size(binary_large) < byte_size(binary)
    end
  end

  describe "primitive encodings" do
    test "encode_long/1 handles small positive values" do
      assert Encoder.encode_long(0) == <<0>>
      assert Encoder.encode_long(1) == <<2>>
      assert Encoder.encode_long(63) == <<126>>
    end

    test "encode_long/1 handles negative values (zigzag)" do
      assert Encoder.encode_long(-1) == <<1>>
      assert Encoder.encode_long(-64) == <<127>>
    end

    test "encode_long/1 handles large values (varint)" do
      # 128 = 10000000 in binary, requires 2 bytes in varint
      assert IO.iodata_to_binary(Encoder.encode_long(64)) == <<128, 1>>

      # Large value
      large_value = 1_000_000
      encoded = IO.iodata_to_binary(Encoder.encode_long(large_value))
      assert is_binary(encoded)
      assert byte_size(encoded) > 1
    end

    test "encode_int/1 works similarly to encode_long/1" do
      assert Encoder.encode_int(0) == <<0>>
      assert Encoder.encode_int(1) == <<2>>
      assert Encoder.encode_int(-1) == <<1>>
    end

    test "encode_string/1 includes length prefix" do
      # Empty string
      assert IO.iodata_to_binary(Encoder.encode_string("")) == <<0>>

      # "foo" = 3 bytes, length=3 -> encode_long(3) = <<6>>
      assert IO.iodata_to_binary(Encoder.encode_string("foo")) == <<6, "foo">>

      # Unicode string
      unicode = "café"
      encoded = IO.iodata_to_binary(Encoder.encode_string(unicode))
      # Encoded should end with the unicode string itself
      unicode_size = byte_size(unicode)
      prefix_size = byte_size(encoded) - unicode_size
      <<_length::binary-size(prefix_size), rest::binary>> = encoded
      assert rest == unicode
    end

    test "encode_bytes/1 includes length prefix" do
      bytes = <<1, 2, 3, 4>>
      encoded = IO.iodata_to_binary(Encoder.encode_bytes(bytes))
      # Length 4 -> encode_long(4) = <<8>>
      assert encoded == <<8, 1, 2, 3, 4>>
    end
  end

  describe "header structure" do
    test "header contains avro.schema metadata" do
      schema = %{
        "type" => "record",
        "name" => "Test",
        "fields" => [%{"name" => "id", "type" => "long"}]
      }

      records = [%{"id" => 1}]
      binary = Encoder.encode(records, schema)

      # Skip magic bytes (4 bytes)
      <<0x4F, 0x62, 0x6A, 0x01, header_and_data::binary>> = binary

      # Header should contain "avro.schema" string
      schema_json = JSON.encode!(schema)
      assert header_and_data =~ "avro.schema"
      assert header_and_data =~ schema_json
    end

    test "header contains avro.codec metadata" do
      schema = %{
        "type" => "record",
        "name" => "Test",
        "fields" => [%{"name" => "id", "type" => "long"}]
      }

      records = [%{"id" => 1}]
      binary = Encoder.encode(records, schema, codec: :null)

      <<0x4F, 0x62, 0x6A, 0x01, header_and_data::binary>> = binary
      assert header_and_data =~ "avro.codec"
      assert header_and_data =~ "null"
    end
  end
end
