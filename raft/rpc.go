package raft

import (
	"net/rpc"
	"sync"
	"time"
)

// RequestVoteArgs and RequestVoteReply carry the RequestVote RPC, by which a
// candidate solicits a vote for a term.
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

// AppendEntriesArgs and AppendEntriesReply carry the AppendEntries RPC, by
// which a leader replicates log entries and heartbeats to a follower.
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
	// ConflictTerm and ConflictIndex extend the basic protocol: on a
	// rejection they let the leader skip a whole mismatched term in one
	// round trip instead of decrementing nextIndex one entry at a time. A
	// ConflictTerm of zero means the follower's log was simply too short.
	ConflictTerm  int
	ConflictIndex int
}

// rpcHandler adapts a Node to the net/rpc method convention. It is registered
// under the service name "Raft", so the wire methods are Raft.RequestVote and
// Raft.AppendEntries.
type rpcHandler struct {
	node *Node
}

func (h *rpcHandler) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	h.node.handleRequestVote(&args, reply)
	return nil
}

func (h *rpcHandler) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	h.node.handleAppendEntries(&args, reply)
	return nil
}

// Register publishes the node's Raft RPC service on srv. The caller owns the
// listener and decides which transport and port to expose it on.
func (n *Node) Register(srv *rpc.Server) error {
	return srv.RegisterName("Raft", &rpcHandler{node: n})
}

// transport holds outbound RPC connections to peers. Connections are dialed
// lazily and cached; a failed call drops its connection so the next attempt
// redials, which transparently recovers from a peer restart.
type transport struct {
	addrs map[int]string

	mu      sync.Mutex
	clients map[int]*rpc.Client
}

func newTransport(cluster map[int]string, selfID int) *transport {
	addrs := make(map[int]string)
	for id, addr := range cluster {
		if id != selfID {
			addrs[id] = addr
		}
	}
	return &transport{addrs: addrs, clients: make(map[int]*rpc.Client)}
}

// call performs a single RPC, bounded by rpcTimeout so that an unresponsive
// peer cannot stall an election or a replication round. It reports success.
func (t *transport) call(peerID int, method string, args, reply any) bool {
	client, err := t.clientFor(peerID)
	if err != nil {
		return false
	}

	pending := client.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case done := <-pending.Done:
		if done.Error != nil {
			t.drop(peerID, client)
			return false
		}
		return true
	case <-time.After(rpcTimeout):
		t.drop(peerID, client)
		return false
	}
}

// clientFor returns a cached connection to a peer, dialing one if necessary.
func (t *transport) clientFor(peerID int) (*rpc.Client, error) {
	t.mu.Lock()
	if c := t.clients[peerID]; c != nil {
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()

	// Dial without the lock held; another goroutine may dial concurrently.
	dialed, err := rpc.Dial("tcp", t.addrs[peerID])
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing := t.clients[peerID]; existing != nil {
		dialed.Close() // lost the race; keep the established connection
		return existing, nil
	}
	t.clients[peerID] = dialed
	return dialed, nil
}

// drop discards a connection that failed, but only if it is still the cached
// one — a concurrent caller may already have replaced it.
func (t *transport) drop(peerID int, client *rpc.Client) {
	t.mu.Lock()
	if t.clients[peerID] == client {
		delete(t.clients, peerID)
	}
	t.mu.Unlock()
	client.Close()
}

// closeAll tears down every peer connection.
func (t *transport) closeAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
}
