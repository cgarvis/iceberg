defmodule Iceberg.Error do
  @moduledoc """
  Centralized error handling for Iceberg operations.

  Provides consistent error tuple creation across the library.
  """

  @type error :: {:error, term()}

  @doc """
  Returns an error indicating the table already exists.

  Used when attempting to create a table that's already present.
  """
  @spec table_exists() :: error()
  def table_exists, do: {:error, :already_exists}

  @doc """
  Returns an error indicating the resource was not found.

  Used when metadata files or tables cannot be located.
  """
  @spec not_found() :: error()
  def not_found, do: {:error, :not_found}

  @doc """
  Returns an error indicating metadata loading failed.

  ## Parameters
    - reason: The underlying failure reason
  """
  @spec metadata_load_failed(term()) :: error()
  def metadata_load_failed(reason), do: {:error, {:metadata_load_failed, reason}}

  @doc """
  Returns an error indicating metadata save failed.

  ## Parameters
    - reason: The underlying failure reason
  """
  @spec metadata_save_failed(term()) :: error()
  def metadata_save_failed(reason), do: {:error, {:metadata_save_failed, reason}}

  @doc """
  Returns an error indicating manifest upload failed.

  ## Parameters
    - reason: The underlying failure reason
  """
  @spec manifest_upload_failed(term()) :: error()
  def manifest_upload_failed(reason), do: {:error, {:manifest_upload_failed, reason}}

  @doc """
  Returns an error indicating manifest-list upload failed.

  ## Parameters
    - reason: The underlying failure reason
  """
  @spec manifest_list_upload_failed(term()) :: error()
  def manifest_list_upload_failed(reason), do: {:error, {:manifest_list_upload_failed, reason}}

  @doc """
  Returns an error indicating COPY operation failed.

  ## Parameters
    - reason: The underlying failure reason
  """
  @spec copy_failed(term()) :: error()
  def copy_failed(reason), do: {:error, {:copy_failed, reason}}

  @doc """
  Returns an error indicating invalid file pattern.

  ## Parameters
    - message: Description of why the pattern is invalid
  """
  @spec invalid_file_pattern(String.t()) :: error()
  def invalid_file_pattern(message), do: {:error, {:invalid_file_pattern, message}}
end
