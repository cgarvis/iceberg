defmodule Iceberg.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/cgarvis/iceberg"

  def project do
    [
      app: :iceberg,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      preferred_cli_env: [precommit: :test],
      package: package(),
      name: "Iceberg",
      description: "Apache Iceberg v2 table format implementation in pure Elixir",
      source_url: @source_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      name: "iceberg",
      description:
        "Apache Iceberg v2 table format implementation in pure Elixir with zero runtime dependencies",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Iceberg",
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end

  defp aliases do
    [
      precommit: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "test",
        "credo --strict",
        "dialyzer"
      ]
    ]
  end
end
