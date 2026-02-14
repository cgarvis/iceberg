defmodule Iceberg.Storage.MemoryTest do
  use ExUnit.Case

  alias Iceberg.Storage.Memory

  setup do
    Memory.clear()
    :ok
  end

  describe "upload/3 and download/1" do
    test "round-trips content" do
      :ok = Memory.upload("test/file.txt", "hello world", [])
      assert {:ok, "hello world"} = Memory.download("test/file.txt")
    end

    test "round-trips binary content" do
      binary = <<1, 2, 3, 4, 5>>
      :ok = Memory.upload("test/binary.dat", binary, [])
      assert {:ok, ^binary} = Memory.download("test/binary.dat")
    end

    test "overwrites existing content" do
      :ok = Memory.upload("test/file.txt", "first", [])
      :ok = Memory.upload("test/file.txt", "second", [])
      assert {:ok, "second"} = Memory.download("test/file.txt")
    end
  end

  describe "download/1" do
    test "returns not_found for missing files" do
      assert {:error, :not_found} = Memory.download("nonexistent")
    end
  end

  describe "list/1" do
    test "lists files with prefix" do
      :ok = Memory.upload("data/a.txt", "a", [])
      :ok = Memory.upload("data/b.txt", "b", [])
      :ok = Memory.upload("meta/c.txt", "c", [])

      result = Memory.list("data/")
      assert length(result) == 2
      assert "data/a.txt" in result
      assert "data/b.txt" in result
    end

    test "returns empty list for no matches" do
      assert Memory.list("nonexistent/") == []
    end
  end

  describe "delete/1" do
    test "removes file" do
      :ok = Memory.upload("test/file.txt", "data", [])
      :ok = Memory.delete("test/file.txt")
      assert {:error, :not_found} = Memory.download("test/file.txt")
    end

    test "returns :ok for nonexistent file" do
      assert :ok = Memory.delete("nonexistent")
    end
  end

  describe "exists?/1" do
    test "returns true for existing file" do
      :ok = Memory.upload("test/file.txt", "data", [])
      assert Memory.exists?("test/file.txt") == true
    end

    test "returns false for missing file" do
      assert Memory.exists?("nonexistent") == false
    end
  end

  describe "clear/0" do
    test "removes all files" do
      :ok = Memory.upload("a.txt", "a", [])
      :ok = Memory.upload("b.txt", "b", [])
      Memory.clear()
      assert Memory.dump() == %{}
    end
  end
end
