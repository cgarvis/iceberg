defmodule Iceberg.SingleValueTest do
  use ExUnit.Case, async: true

  alias Iceberg.SingleValue

  describe "encode/2" do
    test "encodes boolean values" do
      assert SingleValue.encode(false, "boolean") == <<0x00>>
      assert SingleValue.encode(true, "boolean") == <<0x01>>
    end

    test "encodes int as 4-byte little-endian" do
      assert SingleValue.encode(0, "int") == <<0, 0, 0, 0>>
      assert SingleValue.encode(1, "int") == <<1, 0, 0, 0>>
      assert SingleValue.encode(256, "int") == <<0, 1, 0, 0>>
      assert SingleValue.encode(2_147_483_647, "int") == <<255, 255, 255, 127>>
      assert SingleValue.encode(-1, "int") == <<255, 255, 255, 255>>
    end

    test "encodes long as 8-byte little-endian" do
      assert SingleValue.encode(0, "long") == <<0, 0, 0, 0, 0, 0, 0, 0>>
      assert SingleValue.encode(1, "long") == <<1, 0, 0, 0, 0, 0, 0, 0>>

      assert SingleValue.encode(9_223_372_036_854_775_807, "long") ==
               <<255, 255, 255, 255, 255, 255, 255, 127>>

      assert SingleValue.encode(-1, "long") ==
               <<255, 255, 255, 255, 255, 255, 255, 255>>
    end

    test "encodes float as 4-byte IEEE 754 little-endian" do
      # 0.0
      assert SingleValue.encode(0.0, "float") == <<0, 0, 0, 0>>
      # 1.0
      assert SingleValue.encode(1.0, "float") == <<0, 0, 128, 63>>
      # -1.0
      assert SingleValue.encode(-1.0, "float") == <<0, 0, 128, 191>>
    end

    test "encodes double as 8-byte IEEE 754 little-endian" do
      # 0.0
      assert SingleValue.encode(0.0, "double") == <<0, 0, 0, 0, 0, 0, 0, 0>>
      # 1.0
      assert SingleValue.encode(1.0, "double") == <<0, 0, 0, 0, 0, 0, 240, 63>>
      # -1.0
      assert SingleValue.encode(-1.0, "double") == <<0, 0, 0, 0, 0, 0, 240, 191>>
    end

    test "encodes date as days from epoch (4-byte little-endian)" do
      # 1970-01-01 = 0
      assert SingleValue.encode(~D[1970-01-01], "date") == <<0, 0, 0, 0>>
      # 1970-01-02 = 1
      assert SingleValue.encode(~D[1970-01-02], "date") == <<1, 0, 0, 0>>
      # 2024-01-01 = 19723 days
      assert SingleValue.encode(~D[2024-01-01], "date") == <<11, 77, 0, 0>>
    end

    test "encodes timestamp as microseconds from epoch (8-byte little-endian)" do
      # 1970-01-01 00:00:00.000000
      epoch = ~U[1970-01-01 00:00:00.000000Z]
      assert SingleValue.encode(epoch, "timestamp") == <<0, 0, 0, 0, 0, 0, 0, 0>>

      # 1970-01-01 00:00:01.000000 = 1,000,000 microseconds
      one_sec = ~U[1970-01-01 00:00:01.000000Z]
      assert SingleValue.encode(one_sec, "timestamp") == <<64, 66, 15, 0, 0, 0, 0, 0>>
    end

    test "encodes string as UTF-8 bytes (without length)" do
      assert SingleValue.encode("hello", "string") == <<"hello">>
      assert SingleValue.encode("", "string") == <<>>
      assert SingleValue.encode("测试", "string") == <<230, 181, 139, 232, 175, 149>>
    end

    test "encodes binary as raw bytes" do
      assert SingleValue.encode(<<1, 2, 3>>, "binary") == <<1, 2, 3>>
      assert SingleValue.encode(<<>>, "binary") == <<>>
    end

    test "encodes uuid as 16-byte big-endian" do
      # UUID: 12345678-1234-5678-1234-567812345678
      uuid = "12345678-1234-5678-1234-567812345678"

      expected = <<
        0x12,
        0x34,
        0x56,
        0x78,
        0x12,
        0x34,
        0x56,
        0x78,
        0x12,
        0x34,
        0x56,
        0x78,
        0x12,
        0x34,
        0x56,
        0x78
      >>

      assert SingleValue.encode(uuid, "uuid") == expected
    end

    test "returns nil for nil values" do
      assert SingleValue.encode(nil, "int") == nil
      assert SingleValue.encode(nil, "string") == nil
      assert SingleValue.encode(nil, "boolean") == nil
    end

    test "handles integer values for string column IDs" do
      # Column IDs are integers but should encode as 4-byte int
      assert SingleValue.encode(1, "int") == <<1, 0, 0, 0>>
      assert SingleValue.encode(100, "int") == <<100, 0, 0, 0>>
    end
  end

  describe "encode_bounds_map/2" do
    test "encodes a map of column_id => value" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "int"},
          %{"id" => 2, "name" => "name", "type" => "string"}
        ]
      }

      bounds = %{
        1 => 100,
        2 => "Alice"
      }

      result = SingleValue.encode_bounds_map(bounds, schema)

      assert result == %{
               1 => <<100, 0, 0, 0>>,
               2 => <<"Alice">>
             }
    end

    test "returns nil for nil bounds" do
      schema = %{"fields" => []}
      assert SingleValue.encode_bounds_map(nil, schema) == nil
    end

    test "returns empty map for empty bounds" do
      schema = %{"fields" => []}
      assert SingleValue.encode_bounds_map(%{}, schema) == %{}
    end

    test "skips nil values in bounds" do
      schema = %{
        "fields" => [
          %{"id" => 1, "name" => "id", "type" => "int"},
          %{"id" => 2, "name" => "name", "type" => "string"}
        ]
      }

      bounds = %{
        1 => 100,
        2 => nil
      }

      result = SingleValue.encode_bounds_map(bounds, schema)

      assert result == %{
               1 => <<100, 0, 0, 0>>
             }
    end
  end
end
