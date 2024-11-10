defmodule Redis.Server do
  @doc """
  Listen for incoming connections
  """
  def listen() do
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    IO.puts("Logs from your program will appear here!")

    # Since the tester restarts your program quite often, setting SO_REUSEADDR
    # ensures that we don't run into 'Address already in use' errors
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

    if item == nil do
      :gen_tcp.send(client, "$-1\r\n")
    else
      :gen_tcp.send(client, Redis.Protocol.to_bulk_string(item))
    end

    # TODO: Rework this monstrosity
    # if item == nil do
    #   :gen_tcp.send(client, "$-1\r\n")
    # else
    #   {value, expiry} = item

    #   if expiry == nil do
    #     :gen_tcp.send(client, Redis.Protocol.to_bulk_string(value))
    #   else
    #     if :os.system_time(:millisecond) > expiry do
    #       Redis.State.delete(key)
    #       :gen_tcp.send(client, "$-1\r\n")
    #     else
    #       :gen_tcp.send(client, Redis.Protocol.to_bulk_string(value))
    #     end
    #   end
    # end
  end

  def respond_to_command(client, "SET", args) do
    [key_command, value_command] = args
    # [key_command, value_command | options] = args

    key = elem(key_command, 1)
    value = elem(value_command, 1)

    # entry =
    #   case options do
    #     [{:bulk_string, "px"}, {:bulk_string, expiry}] ->
    #       {expiry, _} = Integer.parse(expiry)
    #       {value, :os.system_time(:millisecond) + expiry}

    #     _ ->
    #       {value, nil}
    #   end

    Redis.State.set(key, value)
    # Redis.State.set(key, {value, nil})
    :gen_tcp.send(client, Redis.Protocol.to_simple_string("OK"))
  end

  def respond_to_command(_client, command, _args) do
    IO.puts("Unknown command #{command}")
  end
end
