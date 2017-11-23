defmodule Pontoon do
  require Logger
  use Application

  defp get_env_to_int(name, default) do
    case System.get_env(name) do
      nil -> default
      str -> String.to_integer(str)
    end
  end

  def start(_type, _args) do
    import Supervisor.Spec

    options = [
      rpc_port: get_env_to_int("RPC_PORT", 9213),
      multicast_port: get_env_to_int("MULTICAST_PORT", 8213)
    ]

    children = [
      worker(Pontoon.Raft, [options]),
      worker(Pontoon.Multicast, [options]),
      worker(Pontoon.Membership, []),
    ]

    Logger.info("Initializing...")

    opts = [strategy: :one_for_one, name: Pontoon.Supervisor]
    Supervisor.start_link(children, opts)
  end

end
