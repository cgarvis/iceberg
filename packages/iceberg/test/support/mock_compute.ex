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

  @impl true
  def write_data_files(conn, source_query, dest_path, opts) do
    # For mock purposes, just build a simple COPY statement and execute it
    # This allows tests to mock the COPY command if needed
    partition_by = opts[:partition_by] || []

    partition_clause =
      case partition_by do
        [] -> ""
        parts -> ", PARTITION_BY (#{Enum.join(parts, ", ")})"
      end

    sql = "COPY (#{source_query}) TO '#{dest_path}' (FORMAT PARQUET#{partition_clause})"
    execute(conn, sql)
  end

  defp find_matching_response(responses, sql) do
    Enum.find_value(responses, fn {pattern, response} ->
      if String.contains?(sql, pattern), do: response
    end)
  end
end
