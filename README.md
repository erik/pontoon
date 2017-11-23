# Pontoon

> It's like a Raft.

Toy implementation of the [Raft](http://raft.github.io/) consensus
algorithm, built using Elixir / OTP.

In an effort to keep the scope manageable, membership is discovered simply
through an IGMP multicast group, though theoretically something like SWIM or
`epmd` could be substituted in.

## Setup

``` bash
mix deps.get

# Open a bunch of terminals and then:
RPC_PORT=$RANDOM iex --sname pontoon$RANDOM -S mix
```

## Links

- [Raft whitepaper](https://raft.github.io/raft.pdf)
- [Raft Scope](https://github.com/ongardie/raftscope/blob/master/raft.js)
- [Raft thesis paper](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)
