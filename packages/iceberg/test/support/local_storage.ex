defmodule Iceberg.Storage.Local do
  @moduledoc """
  Filesystem storage backend for integration testing.

  Implements `Iceberg.Storage` behaviour using the local filesystem.
  All paths are relative to a base directory set via `set_base_dir/1`.

  ## Usage

      Iceberg.Storage.Local.start_link()
      Iceberg.Storage.Local.set_base_dir("/tmp/iceberg_test")

      opts = [storage: Iceberg.Storage.Local, base_url: "/tmp/iceberg_test"]
      Iceberg.Table.create("my_table", schema, opts)
  """

  @behaviour Iceberg.Storage

  use Agent

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Agent.start_link(fn -> %{base_dir: nil} end, name: name)
  end

  def set_base_dir(dir, name \\ __MODULE__) do
    Agent.update(name, fn state -> %{state | base_dir: dir} end)
  end

  defp base_dir(name \\ __MODULE__) do
    Agent.get(name, fn state -> state.base_dir end)
  end

  defp full_path(path) do
    Path.join(base_dir(), path)
  end

  @impl true
  def upload(path, content, _opts) do
    dest = full_path(path)
    File.mkdir_p!(Path.dirname(dest))
    File.write!(dest, content)
    :ok
  end

  @impl true
  def download(path) do
    dest = full_path(path)

    case File.read(dest) do
      {:ok, content} -> {:ok, content}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def list(prefix) do
    dir = full_path(prefix)

    if File.dir?(dir) do
      Path.wildcard(Path.join(dir, "**/*"))
      |> Enum.filter(&File.regular?/1)
      |> Enum.map(fn abs_path ->
        Path.relative_to(abs_path, base_dir())
      end)
    else
      []
    end
  end

  @impl true
  def delete(path) do
    dest = full_path(path)
    File.rm(dest)
    :ok
  end

  @impl true
  def exists?(path) do
    dest = full_path(path)
    File.exists?(dest)
  end
end
