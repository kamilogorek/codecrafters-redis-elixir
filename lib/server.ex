defmodule Redis.Server do
  def listen() do
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    accept_connection(socket)
  end

  # TODO: Use `Task.Supervisor.start_child` instead
  def accept_connection(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    Supervisor.start_link([{Task, fn -> receive_packet(client) end}], strategy: :one_for_one)
    accept_connection(socket)
  end

  def receive_packet(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        IO.inspect("Data: #{data}")
        [command | args] = Redis.Protocol.parse(data)
        IO.inspect("Command: #{command}")
        respond_to_command(client, String.upcase(command), args)
        receive_packet(client)

      {:error, :closed} ->
        nil

      {:error, reason} ->
        IO.puts("TCP connection failed: #{reason}")
    end
  end

  def respond_to_command(client, "PING", _args) do
    IO.inspect("Responding to PING")
    :gen_tcp.send(client, Redis.Protocol.to_simple_string("PONG"))
  end

  def respond_to_command(client, "ECHO", []) do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("ECHO command requires value parameter")
    )
  end

  def respond_to_command(client, "ECHO", [value]) do
    IO.inspect("Responding to ECHO: #{value}")
    :gen_tcp.send(client, Redis.Protocol.to_bulk_string(value))
  end

  def respond_to_command(client, "GET", []) do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("GET command requires key parameter")
    )
  end

  def respond_to_command(client, "GET", [key]) do
    case Redis.State.get(key) do
      nil ->
        :gen_tcp.send(client, "$-1\r\n")

      state_value ->
        expiry = Map.get(state_value, :expiry)

        if expiry != nil && :os.system_time(:millisecond) > expiry do
          Redis.State.delete(key)
          :gen_tcp.send(client, "$-1\r\n")
        else
          :gen_tcp.send(client, Redis.Protocol.to_bulk_string(Map.get(state_value, :value)))
        end
    end
  end

  def respond_to_command(client, "TYPE", []) do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("TYPE command requires key parameter")
    )
  end

  def respond_to_command(client, "TYPE", [key]) do
    case Redis.State.get(key) do
      nil ->
        :gen_tcp.send(client, Redis.Protocol.to_simple_string("none"))

      state_value ->
        :gen_tcp.send(
          client,
          Redis.Protocol.to_simple_string(Map.get(state_value, :type, "none"))
        )
    end
  end

  def respond_to_command(client, "SET", args) when length(args) < 2 do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("SET command requires key and value parameters")
    )
  end

  def respond_to_command(client, "SET", [key, value | options]) do
    state_value =
      case options do
        ["px", expiry] ->
          {expiry, _} = Integer.parse(expiry)
          %{type: :string, value: value, expiry: :os.system_time(:millisecond) + expiry}

        _ ->
          %{type: :string, value: value}
      end

    Redis.State.set(key, state_value)
    :gen_tcp.send(client, Redis.Protocol.to_simple_string("OK"))
  end

  def respond_to_command(client, "XADD", []) do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("XADD command requires key parameters")
    )
  end

  def respond_to_command(client, "XADD", [key, id | values]) do
    state_value =
      Redis.State.get(key, %{
        type: :stream,
        entries: []
      })

    prev_id =
      case List.first(state_value[:entries]) do
        nil -> nil
        first_entry -> first_entry[:id]
      end

    id_result =
      cond do
        id == "*" ->
          Redis.Protocol.autogenerated_stream_id(prev_id)

        String.match?(id, ~r"\d+\-\*") ->
          Redis.Protocol.autogenerated_stream_seq(id, prev_id)

        true ->
          Redis.Protocol.explicit_stream_id(id, prev_id)
      end

    case id_result do
      {:ok, next_id} ->
        old_entries = Map.get(state_value, :entries)

        Redis.State.set(key, %{
          state_value
          | entries: [
              %{
                id: next_id,
                values: values
              }
              | old_entries
            ]
        })

        :gen_tcp.send(client, Redis.Protocol.to_bulk_string(next_id))

      {:invalid, nil} ->
        :gen_tcp.send(
          client,
          Redis.Protocol.to_simple_error("ERR The ID specified in XADD must be greater than 0-0")
        )

      {:too_small, nil} ->
        :gen_tcp.send(
          client,
          Redis.Protocol.to_simple_error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
          )
        )
    end
  end

  def respond_to_command(client, "XRANGE", args) when length(args) < 3 do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("XRANGE command requires key, start and end parameters")
    )
  end

  def respond_to_command(client, "XRANGE", [key, start_id, end_id]) do
    state_value =
      Redis.State.get(key, %{
        type: :stream,
        entries: []
      })

    entries = Map.get(state_value, :entries, []) |> Enum.reverse()

    start_index =
      case start_id do
        "-" ->
          0

        _ ->
          Enum.find_index(entries, fn entry ->
            [entry_time, entry_seq] = Redis.Protocol.parse_stream_id(entry[:id])
            [start_time, start_seq] = Redis.Protocol.parse_stream_id(start_id)
            start_time >= entry_time && start_seq <= entry_seq
          end)
      end

    case start_index do
      nil ->
        :gen_tcp.send(client, Redis.Protocol.to_bulk_string_array([]))

      _ ->
        {_, tail} = Enum.split(entries, start_index)

        matched_entries =
          case end_id do
            "+" ->
              tail

            _ ->
              tail
              |> Enum.take_while(fn entry ->
                [entry_time, entry_seq] = Redis.Protocol.parse_stream_id(entry[:id])
                [end_time, end_seq] = Redis.Protocol.parse_stream_id(end_id)
                end_time >= entry_time && end_seq >= entry_seq
              end)
          end
          |> Enum.map(fn entry -> [entry[:id], entry[:values]] end)

        :gen_tcp.send(client, Redis.Protocol.to_bulk_string_array(matched_entries))
    end
  end

  def respond_to_command(client, "XREAD", args) when length(args) < 3 do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("XREAD command requires key and start parameters")
    )
  end

  def respond_to_command(client, "XREAD", [_subcommand | streams]) do
    pairs_count = div(length(streams), 2)

    streams =
      0..(pairs_count - 1)
      |> Enum.map(fn n ->
        {_, tail} = Enum.split(streams, n)
        [key, start_id] = Enum.take_every(tail, pairs_count)

        state_value =
          Redis.State.get(key, %{
            type: :stream,
            entries: []
          })

        entries = Map.get(state_value, :entries, []) |> Enum.reverse()

        start_index =
          Enum.find_index(entries, fn entry ->
            [entry_time, entry_seq] = Redis.Protocol.parse_stream_id(entry[:id])
            [start_time, start_seq] = Redis.Protocol.parse_stream_id(start_id)
            start_time >= entry_time && entry_seq > start_seq
          end)

        case start_index do
          nil ->
            []

          _ ->
            # `XREAD` is exclusive, so +1
            start_index =
              case start_id do
                "0-0" -> 0
                _ -> start_index
              end

            {_, tail} = Enum.split(entries, start_index)

            matched_entries =
              tail
              |> Enum.map(fn entry -> [entry[:id], entry[:values]] end)

            [key, matched_entries]
        end
      end)

    :gen_tcp.send(client, Redis.Protocol.to_bulk_string_array(streams))
  end

  def respond_to_command(client, "KEYS", []) do
    :gen_tcp.send(client, Redis.Protocol.to_simple_error("KEYS command requires key parameter"))
  end

  def respond_to_command(client, "KEYS", ["*"]) do
    :gen_tcp.send(client, Redis.Protocol.to_bulk_string_array(Redis.State.keys()))
  end

  def respond_to_command(client, "KEYS", [key | _]) do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("KEYS command supports * key only. Provided key: #{key}")
    )
  end

  def respond_to_command(client, "CONFIG", args) when length(args) < 2 do
    :gen_tcp.send(client, Redis.Protocol.to_simple_error("CONFIG command requires subcommand"))
  end

  def respond_to_command(client, "CONFIG", [subcommand, key | _]) do
    case String.upcase(subcommand) do
      "GET" ->
        case :ets.lookup(:server_config, key) do
          [{_, value}] ->
            :gen_tcp.send(client, Redis.Protocol.to_bulk_string_array([key, value]))

          [] ->
            :gen_tcp.send(
              client,
              Redis.Protocol.to_simple_error("No config available for key #{key}")
            )
        end

      _ ->
        IO.puts("Unknown CONFIG subcommand #{subcommand}")
    end
  end

  def respond_to_command(_client, command, _args) do
    IO.puts("Unknown command #{command}")
  end
end
