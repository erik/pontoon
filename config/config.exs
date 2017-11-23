use Mix.Config

# How frequently (in ms) to announce self to multicast group
config :pontoon, announce_interval_ms: 3000
config :pontoon, multicast_group_address: {224, 0, 0, 251}

# Maximum time (in ms) that a member can be down before removed from membership
config :pontoon, max_member_age_ms: 20 * 60 * 1000
