# Distkv — Distributed Configuration & Session Store
## Design Document

## Goal
Build a fault-tolerant, strongly consistent distributed store in Go that serves two real production needs: service configuration management and user session storage. Implements Raft consensus from scratch. Academic-level project targeting portfolio use for FAANG-level interviews.

---

## The Problem It Solves

### Problem 1 — Distributed Configuration
In a microservices architecture, services need shared configuration — feature flags, rate limits, connection strings, timeouts. If you store this in a single database, it's a single point of failure. If you store it per-service, changes don't propagate consistently. A config change that reaches 3 out of 5 services is worse than no change at all.

### Problem 2 — Session Storage
User sessions need to survive individual server failures. If your session store is a single Redis node and it goes down, every user gets logged out. You need replication with strong consistency — a session written on one node must be immediately readable on any other node.

### The Insight
Both problems have the same solution: a replicated, strongly consistent key-value store with automatic failover. Distkv is that store.

---

## Tech Stack

| Layer | Choice | Reason |
|---|---|---|
| Language | Go 1.21+ | Native concurrency, goroutines map naturally to Raft's design |
| Consensus | Raft (from scratch) | Most widely understood consensus algorithm, directly explainable in interviews |
| Transport | net/rpc over TCP | No external dependencies, clean Go idiom |
| Storage | In-memory + WAL to disk | Durability without a full database dependency |
| CLI | Cobra | Consistent with existing Go work |
| Namespacing | Key prefixes (config:: session::) | Separate config and session data in one store logically |

---

## Architecture Overview

```
Microservice A          User Request
     |                      |
     v                      v
[Distkv Client SDK]   [Distkv Client SDK]
     |                      |
     +----------+-----------+
                |
     [Distkv Cluster - 3 nodes]
         |           |           |
    [Node 1]    [Node 2]    [Node 3]
    (Leader)  (Follower)  (Follower)
         |           |           |
    [WAL+KV]    [WAL+KV]    [WAL+KV]
```

- All writes route to Leader
- Leader replicates to followers before committing
- Any node fails: cluster elects new leader within 300ms
- Clients reconnect to new leader automatically
- 3-node cluster tolerates 1 failure, 5-node tolerates 2

---

## Key Namespacing

Config and session data live in the same Raft log but are logically separated by key prefix:

```
config::feature_flags::dark_mode       -> "true"
config::rate_limits::api_calls_per_min -> "1000"
config::db::connection_timeout_ms      -> "5000"

session::user_123::token               -> "eyJhbGci..."
session::user_123::last_seen           -> "1716912345"
session::user_456::token               -> "eyJhbGci..."
```

This means one Distkv cluster can serve both use cases simultaneously. Operations teams manage config keys, the auth service manages session keys — same cluster, same consistency guarantees, no interference.

---

## Core Data Structures

### Raft Node
```go
type NodeState int

const (
    Follower  NodeState = iota
    Candidate
    Leader
)

type RaftNode struct {
    id          int
    state       NodeState
    currentTerm int
    votedFor    int
    log         []LogEntry
    commitIndex int
    lastApplied int

    // Leader only
    nextIndex  []int
    matchIndex []int

    // State machine
    store map[string]string

    // TTL support for sessions
    ttl map[string]time.Time

    peers []string
    mu    sync.Mutex
}
```

### Log Entry
```go
type LogEntry struct {
    Term    int
    Index   int
    Command Command
}

type Command struct {
    Op    string // SET, GET, DELETE, SETEX (set with expiry)
    Key   string
    Value string
    TTL   time.Duration // for SETEX, used by session entries
}
```

### RPC Messages

**RequestVote:**
```go
type RequestVoteArgs struct {
    Term         int
    CandidateID  int
    LastLogIndex int
    LastLogTerm  int
}

type RequestVoteReply struct {
    Term        int
    VoteGranted bool
}
```

**AppendEntries:**
```go
type AppendEntriesArgs struct {
    Term         int
    LeaderID     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}
```

---

## Raft Algorithm

### Leader Election
1. Nodes start as Followers with randomized election timeout (150-300ms)
2. No heartbeat received within timeout: become Candidate, increment term, vote for self
3. Send RequestVote to all peers in parallel
4. Majority votes received: become Leader
5. Leader sends heartbeats every 50ms
6. Any node sees higher term: revert to Follower immediately

### Log Replication
1. Client sends command to Leader
2. Leader appends to local log
3. Leader sends AppendEntries to all followers in parallel
4. Majority acknowledge: Leader commits, applies to state machine, responds to client
5. Followers learn of commit on next AppendEntries, apply to their state machines

### TTL Expiry (for sessions)
```go
// Background goroutine on each node
func (n *RaftNode) expireLoop() {
    ticker := time.NewTicker(1 * time.Second)
    for range ticker.C {
        n.mu.Lock()
        for key, expiry := range n.ttl {
            if time.Now().After(expiry) {
                delete(n.store, key)
                delete(n.ttl, key)
            }
        }
        n.mu.Unlock()
    }
}
```

Note: TTL expiry is applied locally on each node after commit. Leader does not need to replicate deletes for expired keys — they expire naturally on all nodes since TTL is stored in the log.

---

## State Machine Operations
```go
func (n *RaftNode) apply(cmd Command) string {
    switch cmd.Op {
    case "SET":
        n.store[cmd.Key] = cmd.Value
        return "OK"
    case "SETEX":
        n.store[cmd.Key] = cmd.Value
        n.ttl[cmd.Key] = time.Now().Add(cmd.TTL)
        return "OK"
    case "GET":
        if expiry, ok := n.ttl[cmd.Key]; ok {
            if time.Now().After(expiry) {
                delete(n.store, cmd.Key)
                delete(n.ttl, cmd.Key)
                return "NOT_FOUND"
            }
        }
        val, ok := n.store[cmd.Key]
        if !ok {
            return "NOT_FOUND"
        }
        return val
    case "DELETE":
        delete(n.store, cmd.Key)
        delete(n.ttl, cmd.Key)
        return "OK"
    case "LIST":
        // list all keys with given prefix
        var keys []string
        for k := range n.store {
            if strings.HasPrefix(k, cmd.Key) {
                keys = append(keys, k)
            }
        }
        return strings.Join(keys, "\n")
    }
    return "UNKNOWN_COMMAND"
}
```

---

## Persistence (Write-Ahead Log)
Persist before applying to survive crashes:
```go
// On every state change, write to WAL atomically
func (n *RaftNode) persist() {
    state := PersistentState{
        CurrentTerm: n.currentTerm,
        VotedFor:    n.votedFor,
        Log:         n.log,
    }
    data, _ := json.Marshal(state)
    // write to temp file, rename atomically
    os.WriteFile("wal.tmp", data, 0600)
    os.Rename("wal.tmp", "wal.log")
}

func (n *RaftNode) restore() {
    data, err := os.ReadFile("wal.log")
    if err != nil { return }
    var state PersistentState
    json.Unmarshal(data, &state)
    n.currentTerm = state.CurrentTerm
    n.votedFor = state.VotedFor
    n.log = state.Log
}
```

---

## Project Structure

```
distkv/
├── go.mod
├── go.sum
├── README.md
├── cmd/
│   ├── server/
│   │   └── main.go          # start a node
│   └── client/
│       └── main.go          # CLI client (Cobra)
├── raft/
│   ├── node.go              # RaftNode struct, core state
│   ├── election.go          # leader election
│   ├── replication.go       # log replication, AppendEntries
│   ├── rpc.go               # RPC server and client
│   └── persistence.go       # WAL
├── store/
│   ├── distkv.go            # public API wrapping Raft
│   └── ttl.go               # TTL expiry loop
└── test/
    ├── election_test.go
    ├── replication_test.go
    ├── session_test.go
    ├── config_test.go
    └── fault_test.go
```

---

## Client SDK (simple wrapper)
```go
type DistkvClient struct {
    leaderAddr string
}

// Config operations
func (c *DistkvClient) SetConfig(key, value string) error
func (c *DistkvClient) GetConfig(key string) (string, error)
func (c *DistkvClient) WatchConfig(key string, onChange func(string)) // poll-based

// Session operations
func (c *DistkvClient) SetSession(userID, token string, ttl time.Duration) error
func (c *DistkvClient) GetSession(userID string) (string, error)
func (c *DistkvClient) DeleteSession(userID string) error
```

---

## Demo Script (for README and interviews)

**Start 3-node cluster:**
```bash
./distkv-server --id 1 --port 8001 --peers localhost:8002,localhost:8003 &
./distkv-server --id 2 --port 8002 --peers localhost:8001,localhost:8003 &
./distkv-server --id 3 --port 8003 --peers localhost:8001,localhost:8002 &
```

**Config use case:**
```bash
./distkv-client set config::feature_flags::dark_mode true
# OK
./distkv-client get config::feature_flags::dark_mode
# true
./distkv-client list config::
# config::feature_flags::dark_mode
# config::rate_limits::api_calls_per_min
```

**Session use case:**
```bash
./distkv-client setex session::user_123::token eyJhbGci... 3600s
# OK (expires in 1 hour)
./distkv-client get session::user_123::token
# eyJhbGci...
```

**Fault tolerance demo:**
```bash
# Kill the leader
kill <node1_pid>

# Wait ~300ms for election
sleep 1

# Cluster still serves requests on new leader
./distkv-client --addr localhost:8002 get config::feature_flags::dark_mode
# true  (data intact, new leader elected)

# Sessions still alive
./distkv-client --addr localhost:8002 get session::user_123::token
# eyJhbGci...
```

---

## Tests to Write

```go
// 1. Leader election in fresh 3-node cluster
// 2. Leader failover within 500ms
// 3. Config replication across all nodes
// 4. Session TTL expiry — key gone after TTL
// 5. Session survives node failure — readable from new leader
// 6. Config change propagates to all nodes before client gets OK
// 7. Network partition — minority partition stops accepting writes
// 8. Node rejoins after partition — catches up on missed entries
// 9. Full cluster restart — all data restored from WAL
```

---

## Phases

**Phase 1 (Day 1-2):** RaftNode, leader election, heartbeats. Stable leader in 3-node cluster.

**Phase 2 (Day 3-4):** Log replication, AppendEntries, commit. SET/GET working end to end.

**Phase 3 (Day 5):** TTL support (SETEX). Session expiry loop.

**Phase 4 (Day 6):** Persistence (WAL). Cluster survives full restart.

**Phase 5 (Day 7):** Cobra CLI client. Client SDK. LIST operation. Tests.

**Phase 6 (Day 8):** Demo script. README with architecture diagram. Clean up.

---

## What to Tell Interviewers

"I built Distkv — a distributed store for service configuration and session state, implementing Raft consensus from scratch in Go. The core insight was that both problems — config consistency across microservices and session durability across servers — share the same solution: a replicated, strongly consistent store. A 3-node Distkv cluster tolerates one node failure, elects a new leader in under 300 milliseconds, and guarantees that a config change or session write is immediately consistent across all nodes before the client gets an OK. Sessions support TTL for automatic expiry. I implemented leader election with randomized timeouts, log replication with majority commit, and a write-ahead log for crash recovery."

---

## Reference
- Raft paper: https://raft.github.io/raft.pdf — read sections 5.1 to 5.4 first
- etcd is the production version of this — worth reading their architecture docs for context
- MIT 6.824 lab 2 — don't copy, use for test case ideas only
