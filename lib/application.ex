# watchexec -w lib -r --stop-signal SIGKILL mix run --no-halt

defmodule Redis do
  use Application

  def start(_type, _args) do
    {opts, _, _} = OptionParser.parse(System.argv(), strict: [dir: :string, dbfilename: :string])

    :ets.new(:server_config, [:named_table])

    Enum.each(Keyword.keys(opts), fn key ->
      :ets.insert(:server_config, {Atom.to_string(key), opts[key]})
    end)

    children =
      if Mix.env() == :test,
        do: [],
        else: [{Task, fn -> Redis.Server.listen() end}, Redis.State]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
