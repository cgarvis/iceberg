defmodule Iceberg.UUID do
  @moduledoc """
  Generates UUID v4 identifiers for Iceberg resources.

  Used for:
  - Table UUIDs
  - Manifest file names
  - Manifest-list file names
  """

  @doc """
  Generates a UUID v4 (random) identifier.

  Uses cryptographically strong random bytes and follows RFC 4122
  for version 4 UUID format.

  ## Returns

  A lowercase UUID string in 8-4-4-4-12 format (e.g., "550e8400-e29b-41d4-a716-446655440000")

  ## Examples

      iex> uuid = Iceberg.UUID.generate()
      iex> String.match?(uuid, ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
      true

  """
  @spec generate() :: String.t()
  def generate do
    # Generate UUID v4
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)

    <<u0::48, 4::4, u1::12, 2::2, u2::62>>
    |> Base.encode16(case: :lower)
    |> String.replace(~r/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, "\\1-\\2-\\3-\\4-\\5")
  end
end
