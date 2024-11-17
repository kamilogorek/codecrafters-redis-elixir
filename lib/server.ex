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

      entry ->
        expiry = Map.get(entry, :expiry)

        if expiry != nil && :os.system_time(:millisecond) > expiry do
          Redis.State.delete(key)
          :gen_tcp.send(client, "$-1\r\n")
        else
          :gen_tcp.send(client, Redis.Protocol.to_bulk_string(Map.get(entry, :value)))
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

      entry ->
        :gen_tcp.send(client, Redis.Protocol.to_simple_string(Map.get(entry, :type, "none")))
    end
  end

  def respond_to_command(client, "SET", args) when length(args) < 2 do
    :gen_tcp.send(
      client,
      Redis.Protocol.to_simple_error("SET command requires key and value parameters")
    )
  end

  def respond_to_command(client, "SET", [key, value | options]) do
    entry =
      case options do
        ["px", expiry] ->
          {expiry, _} = Integer.parse(expiry)
          %{type: :string, value: value, expiry: :os.system_time(:millisecond) + expiry}

        _ ->
          %{type: :string, value: value}
      end

    Redis.State.set(key, entry)
    :gen_tcp.send(client, Redis.Protocol.to_simple_string("OK"))
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
