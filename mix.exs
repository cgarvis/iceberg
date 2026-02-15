defmodule IcebergWorkspace.MixProject do
  use Mix.Project

  def project do
    [
      app: :iceberg_workspace,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      preferred_cli_env: [test_all: :test]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Dependencies for workspace-level tools
  defp deps do
    [
      # Include both packages as path dependencies for workspace development
      {:iceberg, path: "packages/iceberg", override: true},
      {:iceberg_duckdb, path: "packages/iceberg_duckdb"}
    ]
  end

  defp aliases do
    [
      # Test all packages
      test_all: [
        "cmd --cd packages/iceberg mix test",
        "cmd --cd packages/iceberg_duckdb mix test"
      ],
      # Format all packages
      format_all: [
        "cmd --cd packages/iceberg mix format",
        "cmd --cd packages/iceberg_duckdb mix format"
      ],
      # Get deps for all packages
      "deps.get_all": [
        "cmd --cd packages/iceberg mix deps.get",
        "cmd --cd packages/iceberg_duckdb mix deps.get"
      ]
    ]
  end
end
