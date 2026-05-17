// Package raft implements the Raft consensus algorithm — leader election, log
// replication, and crash recovery — from scratch.
//
// The package is deliberately agnostic to both transport and application. The
// committed log drives an opaque StateMachine, and peer communication goes
// through a transport that the package owns but that callers never see. This
// separation keeps the consensus core independently testable and lets the
// store package layer a key-value application on top without entanglement.
package raft

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Command operations recorded in the log. SET, SETEX and DELETE mutate the
// state machine; NOOP carries no mutation and exists only so a new leader can
// commit entries inherited from earlier terms (see becomeLeaderLocked).
const (
	OpSet    = "SET"
	OpSetEx  = "SETEX"
	OpDelete = "DELETE"
	OpNoop   = "NOOP"
)

// Command is a single operation applied to the replicated state machine.
type Command struct {
	Op    string
	Key   string
	Value string
	// ExpireAt is an absolute expiry time in Unix nanoseconds, used by SETEX.
	// The leader stamps it once, at proposal time, so that every replica
	// expires the key at the same instant regardless of when it applies the
	// entry. A zero value means the key never expires.
	ExpireAt int64
}

// LogEntry is a command positioned in the replicated log. Index equals the
// entry's slice position; the log carries a sentinel at index 0 so that
// PrevLogIndex arithmetic never needs a special case.
type LogEntry struct {
	Term    int
	Index   int
	Command Command
}

// StateMachine is the application that the committed log drives. Apply is
// invoked exactly once per committed entry, in log order, on every node, and
// must therefore be deterministic.
type StateMachine interface {
	Apply(cmd Command) string
}

// Tunable timing parameters. The election timeout is an order of magnitude
// larger than the heartbeat interval so that a live leader is never displaced
// by a spurious election.
const (
	heartbeatInterval     = 50 * time.Millisecond
	electionTimeoutMin    = 150 * time.Millisecond
	electionTimeoutMax    = 300 * time.Millisecond
	electionCheckInterval = 10 * time.Millisecond
	rpcTimeout            = 400 * time.Millisecond
	proposeTimeout        = 2 * time.Second
)

// ErrTimeout and ErrStopped report why a proposal did not complete.
var (
	ErrTimeout = errors.New("raft: proposal timed out before it was committed")
	ErrStopped = errors.New("raft: node has been stopped")
)

// NotLeaderError is returned by Propose when the node cannot accept writes.
// LeaderAddr carries the best-known leader address so a client can redirect;
// it is empty when no leader is currently known.
type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "raft: node is not the leader (leader currently unknown)"
	}
	return "raft: node is not the leader (current leader is " + e.LeaderAddr + ")"
}

// pendingProposal links a log index awaiting commit to the caller blocked in
// Propose. term records the leader term under which the entry was appended so
// the applier can detect that leadership changed before the entry committed.
type pendingProposal struct {
	term   int
	result chan proposalResult
}

type proposalResult struct {
	value string
	err   error
}

// Node is a single member of a Raft cluster. All mutable fields are guarded by
// mu; methods whose names end in "Locked" assume the caller already holds it.
type Node struct {
	mu sync.Mutex

	id      int
	cluster map[int]string // node id -> address, for every member; immutable

	// Persistent state — flushed to disk before it is acted upon.
	currentTerm int
	votedFor    int // candidate voted for in currentTerm, or -1 for none
	log         []LogEntry

	// Volatile state.
	state       NodeState
	commitIndex int
	lastApplied int
	leaderID    int // best-known leader, or -1 when unknown
	// readyIndex is the index of the no-op a leader appends on election.
	// Until that entry is applied, the leader's state machine may lag
	// committed history, so the leader must not serve reads yet.
	readyIndex int

	// Volatile leader state, keyed by peer id and rebuilt on every election.
	nextIndex  map[int]int
	matchIndex map[int]int

	electionDeadline time.Time

	sm        StateMachine
	persister *persister
	transport *transport

	// applyObserver, if set, is invoked for every committed entry as it is
	// applied, in log order. It is registered before Start and never changes
	// afterward, so the applier reads it without holding the lock.
	applyObserver func(LogEntry)

	// pending maps a log index to the proposer waiting for it to commit.
	pending map[int]*pendingProposal

	applyCond *sync.Cond // signalled whenever commitIndex advances
	stopCh    chan struct{}
	stopped   bool
}

// NewNode constructs a node for the given cluster and restores any state left
// behind by a previous run. The node does not run until Start is called.
func NewNode(id int, cluster map[int]string, sm StateMachine, dataDir string) (*Node, error) {
	if _, ok := cluster[id]; !ok {
		return nil, errors.New("raft: this node's id is absent from the cluster configuration")
	}

	pst, err := newPersister(dataDir, id)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:         id,
		cluster:    cluster,
		state:      Follower,
		votedFor:   -1,
		leaderID:   -1,
		log:        []LogEntry{{Term: 0, Index: 0}}, // index-0 sentinel
		sm:         sm,
		persister:  pst,
		transport:  newTransport(cluster, id),
		pending:    make(map[int]*pendingProposal),
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),
		stopCh:     make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)

	state, found, err := pst.load()
	if err != nil {
		return nil, err
	}
	if found {
		n.currentTerm = state.CurrentTerm
		n.votedFor = state.VotedFor
		if len(state.Log) > 0 {
			n.log = state.Log
		}
	}
	return n, nil
}

// SetApplyObserver registers a function notified of every committed entry as
// the applier hands it to the state machine, in log order. It is a read-only
// tap on the commit stream: the raft package does not interpret what the
// observer does with the entry. It must be called before Start, and the
// observer must not block, since it runs on the single applier goroutine.
func (n *Node) SetApplyObserver(fn func(LogEntry)) {
	n.applyObserver = fn
}

// Start launches the node's background goroutines: the election timer and the
// state-machine applier. The RPC listener is the caller's responsibility.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionDeadlineLocked()
	n.mu.Unlock()

	go n.electionTicker()
	go n.applier()
}

// Stop halts the node and releases its peer connections. It is idempotent.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.applyCond.Broadcast() // wake the applier so it can exit
	n.mu.Unlock()

	n.transport.closeAll()
}

// ID returns this node's cluster identifier.
func (n *Node) ID() int { return n.id }

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// LeaderReady reports whether this node is a leader whose state machine has
// caught up to the no-op from its current term. A read served before that
// point could miss already-committed history, so callers must gate reads on
// this rather than on IsLeader alone.
func (n *Node) LeaderReady() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader && n.lastApplied >= n.readyIndex
}

// State returns the node's current role.
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the node's current term.
func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LastIndex returns the index of the last log entry. Called immediately after
// NewNode, it reports the highest index restored from the write-ahead log.
func (n *Node) LastIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogIndexLocked()
}

// LeaderAddr returns the address of the best-known leader, or "" if unknown.
func (n *Node) LeaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderAddrLocked()
}

func (n *Node) leaderAddrLocked() string {
	if n.leaderID < 0 {
		return ""
	}
	return n.cluster[n.leaderID]
}

// Propose replicates a command through the log and blocks until it commits.
// It must be called on the leader; otherwise it returns a *NotLeaderError
// carrying a redirect hint.
func (n *Node) Propose(cmd Command) (string, error) {
	n.mu.Lock()
	if n.state != Leader {
		err := &NotLeaderError{LeaderAddr: n.leaderAddrLocked()}
		n.mu.Unlock()
		return "", err
	}

	index := n.appendEntryLocked(cmd)
	result := make(chan proposalResult, 1)
	n.pending[index] = &pendingProposal{term: n.currentTerm, result: result}
	n.broadcastAppendEntriesLocked()
	n.mu.Unlock()

	select {
	case res := <-result:
		return res.value, res.err
	case <-time.After(proposeTimeout):
		n.mu.Lock()
		delete(n.pending, index)
		n.mu.Unlock()
		return "", ErrTimeout
	case <-n.stopCh:
		return "", ErrStopped
	}
}

// applier is a single goroutine that hands committed entries to the state
// machine, in order, and wakes any proposer waiting on each entry. Funnelling
// every Apply through one goroutine guarantees deterministic ordering.
func (n *Node) applier() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}

		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.log[n.lastApplied]

			// Apply outside the lock: the state machine has its own
			// synchronization and may run arbitrary user code.
			n.mu.Unlock()
			var value string
			if entry.Command.Op != OpNoop {
				value = n.sm.Apply(entry.Command)
				if n.applyObserver != nil {
					n.applyObserver(entry)
				}
			}
			n.mu.Lock()

			if waiter, ok := n.pending[entry.Index]; ok {
				delete(n.pending, entry.Index)
				if waiter.term == entry.Term {
					waiter.result <- proposalResult{value: value}
				} else {
					// A different leader's entry occupies this
					// index: the original proposal was lost.
					waiter.result <- proposalResult{
						err: &NotLeaderError{LeaderAddr: n.leaderAddrLocked()},
					}
				}
			}
		}
	}
}

// appendEntryLocked appends a command to the leader's log and persists it.
// A leader counts as its own first replica, so commitment is re-evaluated
// immediately — this also lets a single-node cluster make progress.
func (n *Node) appendEntryLocked(cmd Command) int {
	entry := LogEntry{Term: n.currentTerm, Index: len(n.log), Command: cmd}
	n.log = append(n.log, entry)
	n.persistLocked()
	if n.state == Leader {
		n.advanceCommitLocked()
	}
	return entry.Index
}

// lastLogIndexLocked returns the index of the last log entry; the sentinel
// guarantees this is always at least 0.
func (n *Node) lastLogIndexLocked() int {
	return len(n.log) - 1
}

// quorum is the smallest number of nodes that constitutes a majority.
func (n *Node) quorum() int {
	return len(n.cluster)/2 + 1
}

// peerIDs returns the sorted ids of every cluster member except this node.
// The cluster map is immutable after construction, so no lock is required.
func (n *Node) peerIDs() []int {
	ids := make([]int, 0, len(n.cluster)-1)
	for id := range n.cluster {
		if id != n.id {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	return ids
}

// resetElectionDeadlineLocked arms the election timer with a fresh randomized
// timeout. Randomization is what breaks symmetry between candidates and keeps
// split votes from recurring.
func (n *Node) resetElectionDeadlineLocked() {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	timeout := electionTimeoutMin + time.Duration(rand.Int63n(span))
	n.electionDeadline = time.Now().Add(timeout)
}

// stepDownLocked reverts the node to a follower of a newer term. It is the
// single response to discovering any term greater than the current one.
func (n *Node) stepDownLocked(term int) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = -1
	n.leaderID = -1
	n.persistLocked()
	n.resetElectionDeadlineLocked()
}
