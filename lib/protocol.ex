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
    serialized_values =
      values
      |> Enum.map(fn value ->
        if is_list(value) do
          to_bulk_string_array(value)
        else
          to_bulk_string(value)
        end
      end)
      |> Enum.join("")

    "*#{length(values)}\r\n#{serialized_values}"
  end

  def autogenerated_stream_id(prev_id \\ nil) do
    system_time = :os.system_time(:millisecond)

    case prev_id do
      nil ->
        {:ok, "#{system_time}-0"}

      _ ->
        [prev_time, prev_seq] = parse_stream_id(prev_id)

        case system_time > prev_time do
          true -> {:ok, "#{system_time}-0"}
          false -> {:ok, "#{system_time}-#{prev_seq + 1}"}
        end
    end
  end

  def autogenerated_stream_seq(next_id, prev_id \\ nil) do
    {next_time, _} = next_id |> String.split("-", parts: 2) |> List.first() |> Integer.parse()

    if prev_id == nil do
      case next_time do
        0 -> {:ok, "0-1"}
        _ -> {:ok, "#{next_time}-0"}
      end
    else
      [prev_time, prev_seq] = parse_stream_id(prev_id)

      case next_time > prev_time do
        true -> validate_stream_id("#{next_time}-0", prev_id)
        false -> validate_stream_id("#{next_time}-#{prev_seq + 1}", prev_id)
      end
    end
  end

  def explicit_stream_id(next_id), do: validate_stream_id(next_id)
  def explicit_stream_id(next_id, nil), do: validate_stream_id(next_id)
  def explicit_stream_id(next_id, prev_id), do: validate_stream_id(next_id, prev_id)

  def validate_stream_id(next_id, prev_id \\ nil) do
    if prev_id == nil do
      [next_time, next_seq] = parse_stream_id(next_id)

      cond do
        next_time > 0 -> {:ok, next_id}
        next_seq > 0 -> {:ok, next_id}
        true -> {:invalid, nil}
      end
    else
      [next_time, next_seq] = parse_stream_id(next_id)
      [prev_time, prev_seq] = parse_stream_id(prev_id)

      cond do
        next_time > prev_time -> {:ok, next_id}
        next_time == prev_time && next_seq > prev_seq -> {:ok, next_id}
        next_time == 0 && next_seq == 0 -> {:invalid, nil}
        true -> {:too_small, nil}
      end
    end
  end

  def parse_stream_id(id) do
    id
    |> String.split("-")
    |> Enum.map(fn value ->
      {val, _} = Integer.parse(value)
      val
    end)
  end
end
