defmodule Redis.Server do
  def listen() do
    {:ok, socket} = :gen_tcp.listen(6379, [:binary, active: false, reuseaddr: true])
    accept_connection(socket)
  end

  def accept_connection(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    # TODO: Use `Task.Supervisor.start_child` instead
    Supervisor.start_link([{Task, fn -> receive_packet(client) end}], strategy: :one_for_one)
    accept_connection(socket)
  end

  def receive_packet(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        IO.inspect("Data: #{data}")
        [{_, command} | args] = Redis.Protocol.parse(data)
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

  def respond_to_command(client, "ECHO", args) do
    [{:bulk_string, value}] = args
    IO.inspect("Responding to ECHO: #{value}")
    :gen_tcp.send(client, Redis.Protocol.to_bulk_string(value))
  end

  def respond_to_command(client, "GET", args) do
    [{:bulk_string, key}] = args
    item = Redis.State.get(key)

    case item do
      nil ->
        :gen_tcp.send(client, "$-1\r\n")

      {value, expiry} ->
        if expiry == nil do
          :gen_tcp.send(client, Redis.Protocol.to_bulk_string(value))
        else
          if :os.system_time(:millisecond) > expiry do
            Redis.State.delete(key)
            :gen_tcp.send(client, "$-1\r\n")
          else
            :gen_tcp.send(client, Redis.Protocol.to_bulk_string(value))
          end
        end
    end
  end

  def respond_to_command(client, "SET", args) do
    [key_command, value_command | options] = args

    key = elem(key_command, 1)
    value = elem(value_command, 1)

    entry =
      case options do
        [{:bulk_string, "px"}, {:bulk_string, expiry}] ->
          {expiry, _} = Integer.parse(expiry)
          {value, :os.system_time(:millisecond) + expiry}

        _ ->
          {value, nil}
      end

    Redis.State.set(key, entry)
    :gen_tcp.send(client, Redis.Protocol.to_simple_string("OK"))
  end

  def respond_to_command(client, "CONFIG", args) do
    [{:bulk_string, subcommand}, {:bulk_string, key}] = args

    case String.upcase(subcommand) do
      "GET" ->
        case :ets.lookup(:server_config, key) do
          [{_, value} | _] ->
            :gen_tcp.send(client, Redis.Protocol.to_bulk_string_array([key, value]))

          [] ->
            err = "No config available for key #{key}"
            IO.puts(err)
            :gen_tcp.send(client, Redis.Protocol.to_simple_error(err))
        end

      _ ->
        IO.puts("Unknown CONFIG subcommand #{subcommand}")
    end
  end

  def respond_to_command(_client, command, _args) do
    IO.puts("Unknown command #{command}")
  end
end
