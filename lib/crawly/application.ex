defmodule Crawly.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    storage_opts = Application.get_env(:crawly, :dets_simple_storage, [])
    # Start simple storage of crawly
    Crawly.SimpleStorage.init(storage_opts)
    # Load spiders from SimpleStorage and SPIDERS_DIR
    Crawly.load_spiders()

    import Supervisor.Spec, warn: false
    # List all child processes to be supervised

    children =
      [
        {Crawly.Engine, []},
        {DynamicSupervisor, strategy: :one_for_one, name: Crawly.EngineSup},
        {Crawly.DataStorage, []},
        {Crawly.RequestsStorage, []},
        {DynamicSupervisor,
         strategy: :one_for_one, name: Crawly.RequestsStorage.WorkersSup},
        {DynamicSupervisor,
         strategy: :one_for_one, name: Crawly.DataStorage.WorkersSup}
      ] ++ maybe_enable_http_api()

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Crawly.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp maybe_enable_http_api() do
    case Application.get_env(:crawly, :start_http_api?, false) do
      false ->
        []

      true ->
        port = Application.get_env(:crawly, :port, 4001)

        [
          {Plug.Cowboy,
           scheme: :http, plug: Crawly.API.Router, options: [port: port]}
        ]
    end
  end
end
