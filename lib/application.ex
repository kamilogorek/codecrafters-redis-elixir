# watchexec -w lib -r --stop-signal SIGKILL mix run --no-halt

defmodule Redis do
  use Application

  def start(_type, _args) do
    children =
      if Mix.env() == :test, do: [], else: [{Task, fn -> Redis.Server.listen() end}, Redis.State]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
