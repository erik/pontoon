defmodule Pontoon.Raft do
  require Logger
  use GenServer

  alias Pontoon.RPC, as: RPC

  defmodule State do
    @leader_election_interval_ms 750
    @leader_election_fuzz_factor_ms 250
    @append_entries_interval_ms 500

    defstruct role: :follower, leader: nil, term: 0, log: [], commit_log: [],
      commit_idx: 0, election_timer: nil, leader_state: nil, voted_for: nil

    defmodule Leader do
      defstruct [:votes, :match_idx, :next_idx]
    end

    def reset_election_timer(state, pid) do
      # We add in some noise to the timer duration to make sure the elections
      # don't always play out the same.
      duration = :rand.uniform(@leader_election_fuzz_factor_ms) + @leader_election_interval_ms
      timer = Process.send_after(pid, :leader_election, duration)

      # Clear any existing timer
      if state.election_timer do
        Process.cancel_timer(state.election_timer)
      end

      %{state | election_timer: timer}
    end

    def schedule_append_entries(pid) do
      Process.send_after(pid, {:append_entries, []}, @append_entries_interval_ms)
    end

    def get_log_term(state, index) do
      case Enum.at(state.log, index) do
        nil -> -1
        entry -> entry.term
      end
    end

    def get_last_log_term(state) do
      get_log_term(state, length(state.log) - 1)
    end

    def maybe_step_down(state, member, remote_term) do
      if state.term < remote_term do
        Logger.info("#{member.name} has higher term! Stepping down")
        %{state | term: remote_term, role: :follower, voted_for: nil, leader: member.name}
      else
        state
      end
    end

    # AppendEntries RPC implementation.
    def handle_rpc(state, member, %RPC.AppendEntries{} = rpc) do
      state = maybe_step_down(state, member, rpc.term)
      |> State.reset_election_timer(self())

      {accept_append, state, last_idx} =
        cond do
          rpc.term < state.term ->
            {false, state, -1}

          rpc.prev_log_idx >= length(state.log) ->
            {false, state, length(state.log) - 1}

          # If this is the first entry we receive, everything should check out
          rpc.prev_log_idx == -1 ->
            {true, %{state | log: rpc.entries, commit_idx: rpc.leader_commit}, length(rpc.entries) - 1}

          State.get_log_term(state, rpc.prev_log_idx) != rpc.prev_log_term ->
            # Follow the leader, delete mismatched log items.
            # FIXME: should handle earlier inconsistencies as well.
            log = state.log |> Enum.take(rpc.prev_log_idx - 1)

            {false, %{state | log: log}, length(log) - 1}

          # Success! append entries
          true ->
            log = state.log ++ rpc.entries
            commit =
              if rpc.leader_commit > state.commit_idx do
                # FIXME: unsure if length(log) is correct
                min(rpc.leader_commit, length(log) - 1)
              else
                state.commit_idx
              end

            {true, %{state | log: log, commit_idx: commit}, length(log) - 1}
        end

      reply = %RPC.ReplyAppendEntries{
        term: state.term,
        success: accept_append,
        match_idx: last_idx
      }
      {reply, state}
    end

    # RequestVote RPC implementation
    def handle_rpc(state, member, %RPC.RequestVote{} = rpc) do
      state = maybe_step_down(state, member, rpc.term)
      |> State.reset_election_timer(self())

      vote_granted =
        cond do
          rpc.term < state.term ->
            Logger.info("rejecting #{member.name} for lower term #{rpc.term} vs #{state.term}")
            false

          state.voted_for && state.voted_for != rpc.candidate ->
            Logger.info("rejecting #{member.name} because already voted")
            false

          # FIXME: need to implement commit index correctly
          rpc.last_log_idx < state.commit_idx ->
            Logger.info("rejecting #{member.name} for last_log_idx")
            false

          # Everything checks out!
          true -> true
        end

      state =
        if vote_granted do
          %{state | role: :follower, voted_for: rpc.candidate}
        else
          state
        end

      reply = %RPC.ReplyRequestVote{term: state.term, granted: vote_granted}
      {reply, state}
    end

    def handle_rpc(state, member, %RPC.ReplyRequestVote{} = rpc) do
      Logger.info(">> #{member.name} voted #{rpc.granted}")

      state =
        case state.role do
          # Vote only matters if we're still a candidate
          :candidate ->
            votes =
              if rpc.granted do
                MapSet.put(state.leader_state.votes, member.name)
              else
                state.leader_state.votes
              end

            vote_count = MapSet.size(votes)
            majority_votes = div(Map.size(Pontoon.Membership.list()), 2) + 1

            {role, term} =
                if vote_count >= majority_votes do
                  Logger.warn("Received majority #{vote_count}/#{majority_votes}! Seizing power")
                  {:leader, state.term + 1}
                else
                  Logger.info("Not enough votes. Election not over yet, #{vote_count}/#{majority_votes}")
                  {:candidate, state.term}
                end

            %{state |
              role: role,
              term: term,
              leader_state: %{state.leader_state | votes: votes}}

          _else ->
            state
        end

      {nil, state}
    end

    def handle_rpc(state, _member, %RPC.RequestAppendEntry{} = rpc) do
      if rpc.term != state.term do
        Logger.warn("requestAppendEntry called with outdated term, dropping")
        else
          Logger.info("received append entries request, forwarding to self")
          GenServer.cast(Pontoon.Raft, {:request_append_entry, rpc.value})
          {nil, state}
      end
    end

    def handle_rpc(state, member, %RPC.ReplyAppendEntries{} = rpc) do
      state = maybe_step_down(state, member, rpc.term)
      |> State.reset_election_timer(self())

      case state.role do
        :leader ->
          leader_state =
          if rpc.success do
            match_idx = state.leader_state.match_idx
            |> Map.update(member.name, 0, fn last_match_idx ->
              max(last_match_idx, rpc.match_idx)
            end)

            next_idx = state.leader_state.next_idx
            |> Map.put(member.name, rpc.match_idx + 1)

            %{state.leader_state | match_idx: match_idx, next_idx: next_idx}
          else
            Logger.info("failed to append entries, rolling back next_idx: #{inspect rpc}")

            next_idx = state.leader_state.next_idx
            |> Map.update(member.name, 0, fn idx ->
              max(0, idx - 1)
            end)

            %{state.leader_state | next_idx: next_idx}
          end

          {nil, %{state | leader_state: leader_state}}

        _ ->
          Logger.warn("received replyAppendEntries while not leading, dropping.")
          {nil, state}
      end
    end
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, [name: __MODULE__])
  end

  def init(opts) do
    Logger.info("Starting Raft on port #{inspect opts[:rpc_port]}")
    {:ok, _socket} = :gen_udp.open(opts[:rpc_port], [
          :binary,
          ip: {0, 0, 0, 0},
          active: true,
          reuseaddr: true,
        ])

    state = %State{} |> State.reset_election_timer(self())
    State.schedule_append_entries(self())

    {:ok, state}
  end

  def append_log(log_entry) do
    GenServer.cast(__MODULE__, {:request_append_entry, log_entry})
  end

  def get_log() do
    GenServer.call(__MODULE__, :get_log)
  end

  def send_broadcast(message) do
    RPC.encode(message) |> Pontoon.Membership.send_broadcast
  end

  def send_to(%Pontoon.Member{} = member, message) do
    encoded = RPC.encode(message)
    Pontoon.Membership.send_to(member, encoded)
  end

  def handle_call(:get_log, _from, state) do
    {:reply, state.log, state}
  end

  def handle_cast({:request_append_entry, log_entry}, state) do
    entry = %RPC.RequestAppendEntry{value: log_entry, term: state.term}

    case state.role do
      :leader ->
        Logger.info("I am the leader, appending: #{inspect entry}")
        handle_info({:append_entries, [entry]}, state)

      :follower ->
        Logger.info("request_append_entry called, forwarding to leader #{inspect state.leader}")

        encoded = RPC.encode(entry)
        Pontoon.Membership.send_to(state.leader, encoded)
        {:noreply, state}

      :candidate ->
        {:error, "no leader known, can't commit log entry right now"}
    end
  end

  def handle_info({:udp, _socket, _ip, _port, data}, state) do
    case RPC.decode!(data) do
      {_msg, nil} ->
        Logger.info(">> unknown member, skipping this message")
        {:noreply, state}

      {msg, member} ->
        {reply, state} = State.handle_rpc(state, member, msg)

        if reply, do: send_to(member, reply)

        {:noreply, state}
    end
  end

  def handle_info({:append_entries, entries}, state) do
    State.schedule_append_entries(self())

    case state.role do
      :leader ->
        log = state.log ++ entries

        Pontoon.Membership.list()
        |> Enum.map(fn {name, member} ->
          prev_log_idx = state.leader_state.match_idx |> Map.get(name, -1)
          next_idx = state.leader_state.next_idx |> Map.get(name, 0)
          prev_term = State.get_log_term(state, prev_log_idx)

          entries = log |> Enum.slice(next_idx..-1)

          {member, %RPC.AppendEntries{
            term: state.term,
            leader: Pontoon.Membership.get_own_name(),
            prev_log_idx: next_idx - 1,
            prev_log_term: prev_term,
            leader_commit: state.commit_idx,
            entries: entries
          }}
        end)
        |> Enum.map(fn {member, message} -> send_to(member, message) end)

        {:noreply, %{state | log: log}}

      _else ->
        {:noreply, state}
    end
  end

  def handle_info(:leader_election, state) do
    state = State.reset_election_timer(state, self())

    state =
      case state.role do
        # Nothing to do if we're already the leader
        :leader ->
          Logger.info("Already leader... staying in power")
          state

        # If we became a candidate in the previous cycle, use this
        # cycle to count votes and declare election results
        :candidate ->
          votes = state.leader_state.votes |> MapSet.size
          majority_votes = div(Map.size(Pontoon.Membership.list()), 2) + 1

          Logger.info("Lost election (timeout)... #{votes} votes. (needed: #{majority_votes})")
          %{state | voted_for: nil, role: :follower}

        # Initiate possible regime change.
        :follower ->
          members = Pontoon.Membership.list()

          state = %{state |
                    role: :candidate,
                    voted_for: Pontoon.Membership.get_own_name(),
                    term: state.term + 1,
                    leader_state: %State.Leader{
                      votes: MapSet.new,
                      match_idx: members |> Enum.map(&{&1, -1}) |> Enum.into(%{}),
                      next_idx: members |> Enum.map(&{&1, 0}) |> Enum.into(%{})
                    },
                   }

          vote_message = %RPC.RequestVote{
            term: state.term,
            candidate: Pontoon.Membership.get_own_name(),
            last_log_idx: length(state.log),
            last_log_term: State.get_last_log_term(state)
          }

          Logger.warn("No leader detected! Starting election.")

          send_broadcast(vote_message)

          state
    end

    {:noreply, state}
  end
end
