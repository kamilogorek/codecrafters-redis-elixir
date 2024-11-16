defmodule Redis.State do
  use Agent
  import Bitwise

  def start_link(_opts) do
    dir =
      case :ets.lookup(:server_config, "dir") do
        [{_, value}] -> value
        [] -> "."
      end

    dbfilename =
      case :ets.lookup(:server_config, "dbfilename") do
        [{_, value}] -> value
        [] -> nil
      end

    initial_state =
      case dbfilename do
        nil ->
          %{}

        _ ->
          path = Path.join(dir, dbfilename)
          # TODO: Change to streaming instead of loading all in memory
          case File.read(path) do
            {:ok, contents} ->
              contents |> parse_header() |> parse_body(%{})

            {:error, :enoent} ->
              IO.puts("No dump available at path #{path}. Skipped database loading.")
              %{}
          end
      end

    Agent.start_link(
      fn ->
        initial_state
      end,
      name: __MODULE__
    )
  end

  def get(key) do
    Agent.get(__MODULE__, &Map.get(&1, key))
  end

  def set(key, value) do
    Agent.update(__MODULE__, &Map.put(&1, key, value))
  end

  def delete(key) do
    Agent.update(__MODULE__, &Map.delete(&1, key))
  end

  def keys() do
    Agent.get(__MODULE__, &Map.keys(&1))
  end

  defp parse_header(<<"REDIS0011", rest::binary>>), do: rest

  defp parse_metadata(contents, payload) do
    {_key, rest} = parse_string(contents)
    {_value, rest} = parse_string(rest)
    {rest, payload}
  end

  defp parse_body(<<>>, payload), do: payload

  defp parse_body(<<op::unsigned-integer-size(8), rest::binary>>, payload) do
    case op do
      # Metadata, ignored for now
      0xFA ->
        {rest, payload} = parse_metadata(rest, payload)
        parse_body(rest, payload)

      # Database section with index, ignored for now
      0xFE ->
        {:ok, _db_index, rest} = parse_length(rest)
        parse_body(rest, payload)

      # Hash table size, ignored for now
      0xFB ->
        {:ok, _, rest} = parse_length(rest)
        {:ok, _, rest} = parse_length(rest)
        parse_body(rest, payload)

      # Key/value pair
      0x00 ->
        {key, rest} = parse_string(rest)
        {value, rest} = parse_string(rest)
        payload = Map.put(payload, key, {value, :never})
        parse_body(rest, payload)

      # Key/value pair with expire in milliseconds
      0xFC ->
        <<exp::little-unsigned-integer-size(64), _::size(8), rest::binary>> = rest
        {key, rest} = parse_string(rest)
        {value, rest} = parse_string(rest)
        payload = Map.put(payload, key, {value, exp})
        parse_body(rest, payload)

      # Key/value pair with expire in seconds
      0xFD ->
        <<exp::little-unsigned-integer-size(32), _::size(8), rest::binary>> = rest
        {key, rest} = parse_string(rest)
        {value, rest} = parse_string(rest)
        payload = Map.put(payload, key, {value, exp * 1000})
        parse_body(rest, payload)

      # EOF
      0xFF ->
        parse_body(<<>>, payload)

      _ ->
        IO.puts("Unknown op #{IO.inspect(Integer.to_string(op, 16))}")
    end
  end

  defp parse_string(contents) do
    case parse_length(contents) do
      {:ok, length, rest} ->
        <<value::binary-size(length), rest::binary>> = rest
        {value, rest}

      {:special_encoding, size, rest} ->
        <<value::little-signed-integer-size(size), rest::binary>> = rest
        {Integer.to_string(value), rest}
    end
  end

  defp parse_length(<<encoding_byte::unsigned-integer-size(8), rest::binary>>) do
    encoding_bits = encoding_byte >>> 6

    case encoding_bits do
      0b00 ->
        length = encoding_byte &&& 0b00111111
        {:ok, length, rest}

      0b01 ->
        <<additional_byte::unsigned-integer-size(8), rest::binary>> = rest
        first_chunk = encoding_byte &&& 0b00111111
        length = (first_chunk <<< 8) + additional_byte
        {:ok, length, rest}

      0b10 ->
        <<length::unsigned-integer-size(32), rest::binary>> = rest
        {:ok, length, rest}

      0b11 ->
        special_encoding_chunk = encoding_byte &&& 0b00111111

        case special_encoding_chunk do
          0b000000 ->
            {:special_encoding, 8, rest}

          0b000001 ->
            {:special_encoding, 16, rest}

          0b000010 ->
            {:special_encoding, 32, rest}

          0b000011 ->
            raise ArgumentError,
              message: "compressed strings encoding is not supported"

          _ ->
            raise ArgumentError,
              message:
                "unknown string encoding encountered #{Integer.to_string(special_encoding_chunk, 2)}"
        end
    end
  end
end
