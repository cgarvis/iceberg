defmodule IcebergDuckdb.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/cgarvis/iceberg"

  def project do
    [
      app: :iceberg_duckdb,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "IcebergDuckdb",
      description: "DuckDB compute adapter for Apache Iceberg using ADBC",
      source_url: @source_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:iceberg, path: "../iceberg"},
      {:adbc, "~> 0.6"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      name: "iceberg_duckdb",
      description: "DuckDB compute adapter for Apache Iceberg using ADBC",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "IcebergDuckdb",
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
