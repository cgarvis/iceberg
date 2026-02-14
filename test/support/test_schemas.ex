defmodule Iceberg.Test.Schemas.Events do
  use Iceberg.Schema

  schema "canonical/events" do
    field(:id, :string, required: true)
    field(:timestamp, :timestamp, required: true)
    field(:event_type, :string)
    field(:data, :string)
    field(:partition_date, :date, required: true)

    partition(day(:partition_date))
  end
end

defmodule Iceberg.Test.Schemas.Simple do
  use Iceberg.Schema

  schema "canonical/simple" do
    field(:id, :string, required: true)
    field(:name, :string)
    field(:count, :long)
  end
end

defmodule Iceberg.Test.Schemas.AllPrimitiveTypes do
  use Iceberg.Schema

  schema "canonical/all_primitive_types" do
    # Basic types
    field(:str_field, :string, required: true)
    field(:int_field, :int)
    field(:long_field, :long)
    field(:float_field, :float)
    field(:double_field, :double)
    field(:boolean_field, :boolean)

    # Date/time types
    field(:date_field, :date)
    field(:time_field, :time)
    field(:timestamp_field, :timestamp)
    field(:timestamptz_field, :timestamptz)

    # Other primitive types
    field(:uuid_field, :uuid)
    field(:binary_field, :binary)
    field(:decimal_field, {:decimal, 10, 2})
    field(:fixed_field, {:fixed, 16})
  end
end
