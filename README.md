# Distkv

Consistent distributed key-value store written in Go, with the Raft consensus algorithm implemented from scratch. 

One Distkv cluster serves two production needs at once — **service configuration** and **user session storage** — because both reduce to the same primitive: a replicated store with automatic failover.

A three-node cluster tolerates one node failure, elects a new leader in well
under a second, and guarantees that a write is consistent across a majority of
nodes before the client is told it succeeded.

## Why one store for two problems

- **Configuration** — feature flags, rate limits, timeouts. A change that
  reaches some services but not others is worse than no change at all; config
  needs a single consistent source of truth that no single failure can take
  down.
- **Sessions** — a session written on one node must be readable on any node,
  and must survive that node going away.

Both are a replicated, strongly consistent key-value store with failover.
Configuration and session data share one cluster while staying logically
separate behind key prefixes (`config::`, `session::`); the typed client SDK
enforces the convention.

## Architecture

```
        Microservice                 Auth service
             |                            |
       [client SDK]                 [client SDK]
             \                          /
              \------- locate leader --/
                          |
              +-----------+-----------+
              |           |           |
          [Node 1]    [Node 2]    [Node 3]
          (leader)   (follower)  (follower)
              |           |           |
         state file  state file  state file   (persistent log)
```

Writes route to the leader, which replicates them to followers and commits
once a majority has the entry. If the leader fails, the remaining nodes elect a
new one and clients redirect automatically.



## Build

Requires Go 1.21 or newer.

```sh
go build -o distkv-server ./cmd/server
go build -o distkv-client ./cmd/client
```

## Run a three-node cluster

Every node is given the same `--peers` list — the whole cluster as
`id@host:port` entries — and its own `--id` and data directory.

```sh
./distkv-server --id 1 --peers 1@localhost:8001,2@localhost:8002,3@localhost:8003 --data-dir data/n1 &
./distkv-server --id 2 --peers 1@localhost:8001,2@localhost:8002,3@localhost:8003 --data-dir data/n2 &
./distkv-server --id 3 --peers 1@localhost:8001,2@localhost:8002,3@localhost:8003 --data-dir data/n3 &
```

