defmodule IcebergS3Test do
  use ExUnit.Case

  # These are integration tests that require S3 access.
  # Set @moduletag :skip to disable by default, or configure test S3 bucket.
  @moduletag :skip

  @test_bucket System.get_env("TEST_S3_BUCKET", "iceberg-test")
  @test_prefix "test/iceberg_s3/#{System.unique_integer([:positive])}"

  setup do
    # Clean up any existing test files
    base_url = "s3://#{@test_bucket}/#{@test_prefix}"
    on_exit(fn -> cleanup_test_files(base_url) end)

    {:ok, base_url: base_url}
  end

  describe "upload/3 and download/1" do
    test "round-trips content", %{base_url: base_url} do
      path = "test/file.txt"
      opts = [base_url: base_url]

      :ok = IcebergS3.upload(path, "hello world", opts)
      assert {:ok, "hello world"} = IcebergS3.download(path)
    end

    test "round-trips binary content", %{base_url: base_url} do
      binary = <<1, 2, 3, 4, 5>>
      path = "test/binary.dat"
      opts = [base_url: base_url]

      :ok = IcebergS3.upload(path, binary, opts)
      assert {:ok, ^binary} = IcebergS3.download(path)
    end

    test "supports custom content type", %{base_url: base_url} do
      path = "test/metadata.json"
      content = ~s({"test": "data"})
      opts = [base_url: base_url, content_type: "application/json"]

      :ok = IcebergS3.upload(path, content, opts)
      assert {:ok, ^content} = IcebergS3.download(path)
    end

    test "overwrites existing content", %{base_url: base_url} do
      path = "test/file.txt"
      opts = [base_url: base_url]

      :ok = IcebergS3.upload(path, "first", opts)
      :ok = IcebergS3.upload(path, "second", opts)
      assert {:ok, "second"} = IcebergS3.download(path)
    end
  end

  describe "download/1" do
    test "returns not_found for missing files" do
      path = "s3://#{@test_bucket}/#{@test_prefix}/nonexistent"
      assert {:error, :not_found} = IcebergS3.download(path)
    end
  end

  describe "list/1" do
    test "lists files with prefix", %{base_url: base_url} do
      opts = [base_url: base_url]

      :ok = IcebergS3.upload("data/a.txt", "a", opts)
      :ok = IcebergS3.upload("data/b.txt", "b", opts)
      :ok = IcebergS3.upload("meta/c.txt", "c", opts)

      # List with base_url prefix
      result = IcebergS3.list("#{base_url}/data/")

      assert length(result) == 2
      assert Enum.any?(result, &String.ends_with?(&1, "/data/a.txt"))
      assert Enum.any?(result, &String.ends_with?(&1, "/data/b.txt"))
    end

    test "returns empty list for no matches" do
      result = IcebergS3.list("s3://#{@test_bucket}/#{@test_prefix}/nonexistent/")
      assert result == []
    end
  end

  describe "delete/1" do
    test "removes file", %{base_url: base_url} do
      path = "test/file.txt"
      opts = [base_url: base_url]

      :ok = IcebergS3.upload(path, "data", opts)
      :ok = IcebergS3.delete("#{base_url}/#{path}")
      assert {:error, :not_found} = IcebergS3.download(path)
    end

    test "returns :ok for nonexistent file" do
      path = "s3://#{@test_bucket}/#{@test_prefix}/nonexistent"
      assert :ok = IcebergS3.delete(path)
    end
  end

  describe "exists?/1" do
    test "returns true for existing file", %{base_url: base_url} do
      path = "test/file.txt"
      opts = [base_url: base_url]

      :ok = IcebergS3.upload(path, "data", opts)
      assert IcebergS3.exists?("#{base_url}/#{path}") == true
    end

    test "returns false for missing file" do
      path = "s3://#{@test_bucket}/#{@test_prefix}/nonexistent"
      assert IcebergS3.exists?(path) == false
    end
  end

  # Helper functions

  defp cleanup_test_files(base_url) do
    try do
      # List and delete all test files
      files = IcebergS3.list(base_url)

      Enum.each(files, fn file ->
        IcebergS3.delete("s3://#{@test_bucket}/#{file}")
      end)
    rescue
      _ -> :ok
    end
  end
end
