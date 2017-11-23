# Structs defining the format of RPC messages.
defmodule Pontoon.RPC do
  require Logger

  @derive [Poison.Encoder]
  defstruct [:type, :name, :data]

  defmodule RequestVote do
    @derive [Poison.Encoder]
    defstruct [:term, :candidate, :last_log_idx, :last_log_term]
  end

  defmodule AppendEntries do
    @derive [Poison.Encoder]
    defstruct [:term, :leader, :prev_log_idx, :prev_log_term, :leader_commit, :entries]
  end

  defmodule ReplyRequestVote do
    @derive [Poison.Encoder]
    defstruct [:term, :granted]
  end

  defmodule ReplyAppendEntries do
    @derive [Poison.Encoder]
    defstruct [:term, :success]
  end

  # FIXME: This feels redundant and gross
  def decode!(raw) do
    rpc = Poison.decode!(raw, as: %__MODULE__{})
    member = Pontoon.Membership.get_member(rpc.name)

    msg =
      case rpc.type do
        "AppendEntries" ->
          Poison.decode!(rpc.data, as: %AppendEntries{})
        "ReplyAppendEntries" ->
          Poison.decode!(rpc.data, as: %ReplyAppendEntries{})
        "RequestVote" ->
          Poison.decode!(rpc.data, as: %RequestVote{})
        "ReplyRequestVote" ->
          Poison.decode!(rpc.data, as: %ReplyRequestVote{})
        unknown         ->
          Logger.error("unknown RPC message type: #{inspect unknown}")
          nil
      end

    {msg, member}
  end

  def encode(message) do
    # FIXME: this sucks
    [name] = message.__struct__
    |> Module.split
    |> Enum.take(-1)

    encode(name, Poison.encode!(message))
  end

  def encode(type, data) do
    name = Pontoon.Membership.get_own_name()

    Poison.encode!(%__MODULE__{type: type, data: data, name: name})
  end
end
