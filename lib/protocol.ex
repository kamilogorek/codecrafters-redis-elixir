defmodule Redis.Protocol do
  def parse(data, commands \\ [])

  def parse(data, commands) when data == "" do
    case Enum.reverse(commands) do
      [length | rest] -> Enum.take(rest, length)
      cmds -> cmds
    end
  end

  def parse(data, commands) do
    [data_head, data_tail] = String.split(data, "\r\n", parts: 2)
    {command, remaining_data} = parse_command(data_head, data_tail)
    parse(remaining_data, [command | commands])
  end

  def parse_command("+" <> command_value, args) do
    {command_value, args}
  end

  def parse_command("$" <> command_value, args) do
    {length, _} = Integer.parse(command_value)
    # Account for \r\n and -1 for 0-based index
    {value, rest} = String.split_at(args, length + 1)
    value = String.trim(value)
    {value, rest}
  end

  def parse_command("*" <> command_value, args) do
    {length, _} = Integer.parse(command_value)
    {length, args}
  end

  def to_simple_error(value) do
    if value == "" do
      raise ArgumentError, message: "simple_error does not support empty strings"
    end

    "-#{value}\r\n"
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

  def validate_stream_id(next_id, prev_id \\ nil) do
    if prev_id == nil do
      [next_time, next_seq] =
        String.split(next_id, "-")
        |> Enum.map(fn value ->
          {val, _} = Integer.parse(value)
          val
        end)

      cond do
        next_time > 0 -> :ok
        next_seq > 0 -> :ok
        true -> :invalid
      end
    else
      [next_time, next_seq] =
        String.split(next_id, "-")
        |> Enum.map(fn value ->
          {val, _} = Integer.parse(value)
          val
        end)

      [prev_time, prev_seq] =
        String.split(prev_id, "-")
        |> Enum.map(fn value ->
          {val, _} = Integer.parse(value)
          val
        end)

      cond do
        next_time > prev_time -> :ok
        next_time == prev_time && next_seq > prev_seq -> :ok
        next_time == 0 && next_seq == 0 -> :invalid
        true -> :too_small
      end
    end
  end
end
