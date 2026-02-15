defmodule Iceberg.Test.MockCompute do
  @moduledoc """
  Agent-based mock implementing `Iceberg.Compute` behaviour.

  Allows setting canned query results by SQL pattern matching.

  ## Usage

      Iceberg.Test.MockCompute.set_response("parquet_metadata", {:ok, [%{"file_path" => "..."}]})
      {:ok, results} = Iceberg.Test.MockCompute.query(:conn, "SELECT ... FROM parquet_metadata(...)")
  """

  @behaviour Iceberg.Compute

  use Agent

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Agent.start_link(fn -> %{} end, name: name)
  end

  @doc """
  Sets a canned response for queries matching a pattern.

  The pattern is matched as a substring of the SQL query.
  """
  def set_response(pattern, response, name \\ __MODULE__) do
    Agent.update(name, &Map.put(&1, pattern, response))
  end

  @doc """
  Clears all canned responses.
  """
  def clear(name \\ __MODULE__) do
    Agent.update(name, fn _ -> %{} end)
  end

  @impl true
  def query(_conn, sql) do
    responses = Agent.get(__MODULE__, & &1)

    case find_matching_response(responses, sql) do
      nil -> {:error, :no_mock_response}
      response -> response
    end
  end

  @impl true
  def execute(_conn, sql) do
    responses = Agent.get(__MODULE__, & &1)

    case find_matching_response(responses, sql) do
      nil -> {:ok, :executed}
      response -> response
    end
  end

  defp find_matching_response(responses, sql) do
    Enum.find_value(responses, fn {pattern, response} ->
      if String.contains?(sql, pattern), do: response
    end)
  end
end
