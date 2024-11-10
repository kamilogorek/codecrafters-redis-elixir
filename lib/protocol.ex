defmodule Redis.Protocol do
  def parse(data, commands \\ [])

  def parse(data, commands) when data == "" do
    case Enum.reverse(commands) do
      [{:array, length} | rest] -> Enum.take(rest, length)
      cmds -> cmds
    end
  end

  def parse(data, commands) do
    [data_head, data_tail] = String.split(data, "\r\n", parts: 2)
    {command, remaining_data} = parse_command(data_head, data_tail)
    parse(remaining_data, [command | commands])
  end

  def parse_command("+" <> command_value, args) do
    {{:simple_string, command_value}, args}
  end

  def parse_command("$" <> command_value, args) do
    {length, _} = Integer.parse(command_value)
    # Account for \r\n and -1 for 0-based index
    {value, rest} = String.split_at(args, length + 1)
    value = String.trim(value)
    {{:bulk_string, value}, rest}
  end

  def parse_command("*" <> command_value, args) do
    {length, _} = Integer.parse(command_value)
    {{:array, length}, args}
  end

  def to_simple_string(value) do
    if value == "" do
      raise ArgumentError, message: "simple_string does not support empty strings"
    end

    "+#{value}\r\n"
  end

  def to_bulk_string(value) do
    "$#{String.length(value)}\r\n#{value}\r\n"
  end

  def to_bulk_string_array(values) do
    serialized_values = values |> Enum.map(&to_bulk_string/1) |> Enum.join("")
    "*#{length(values)}\r\n#{serialized_values}"
  end
end
