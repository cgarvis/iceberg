defmodule Iceberg.Storage.Memory do
  @moduledoc """
  In-memory storage backend for testing Iceberg operations.

  Useful for unit tests where you don't want to interact with real storage.

  ## Usage

      # Start the agent
      {:ok, _pid} = Iceberg.Storage.Memory.start_link()

      # Use in tests
      opts = [storage: Iceberg.Storage.Memory, base_url: "memory://test"]
      Iceberg.Table.create(schema, nil, opts)

      # Clear all data between tests
      Iceberg.Storage.Memory.clear()
  """

  @behaviour Iceberg.Storage

  use Agent

  @doc """
  Starts the in-memory storage agent.
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Agent.start_link(fn -> %{} end, name: name)
  end

  @doc """
  Clears all stored data.

  Useful for cleaning up between tests.
  """
  def clear(name \\ __MODULE__) do
    Agent.update(name, fn _ -> %{} end)
  end

  @doc """
  Returns all stored paths and their contents.

  Useful for debugging tests.
  """
  def dump(name \\ __MODULE__) do
    Agent.get(name, & &1)
  end

  @impl true
  def upload(path, content, _opts) do
    Agent.update(__MODULE__, &Map.put(&1, path, content))
    :ok
  end

  @impl true
  def download(path) do
    case Agent.get(__MODULE__, &Map.get(&1, path)) do
      nil -> {:error, :not_found}
      content -> {:ok, content}
    end
  end

  @impl true
  def list(prefix) do
    Agent.get(__MODULE__, fn state ->
      state
      |> Map.keys()
      |> Enum.filter(&String.starts_with?(&1, prefix))
    end)
  end

  @impl true
  def delete(path) do
    Agent.update(__MODULE__, &Map.delete(&1, path))
    :ok
  end

  @impl true
  def exists?(path) do
    Agent.get(__MODULE__, &Map.has_key?(&1, path))
  end
end
