defmodule IcebergS3.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/cgarvis/iceberg"

  def project do
    [
      app: :iceberg_s3,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      name: "IcebergS3",
      description: "S3 storage adapter for Apache Iceberg",
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
      {:ex_aws, "~> 2.5"},
      {:ex_aws_s3, "~> 2.5"},
      {:hackney, "~> 1.20"},
      {:sweet_xml, "~> 0.7"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      name: "iceberg_s3",
      description: "S3 storage adapter for Apache Iceberg",
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url
      },
      files: ~w(lib mix.exs README.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "IcebergS3",
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end
