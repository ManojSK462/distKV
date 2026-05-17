// Package store layers a key-value application on top of the raft package. It
// supplies the replicated state machine, a background expiry reaper for
// session TTLs, and the client-facing RPC service that routes writes through
// consensus and serves reads from the local replica.
package store

import (
	"errors"
	"fmt"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"

	"distkv/raft"
)

// Key prefixes by which configuration and session data share one cluster
// while staying logically separate. The store treats keys as opaque bytes;
// the prefixes are a convention enforced by the typed client SDK.
const (
	ConfigPrefix  = "config::"
	SessionPrefix = "session::"
)

// Operation names accepted by the Execute RPC. SET, SETEX and DELETE travel
// through the Raft log; GET and LIST are served directly by the leader.
const (
	OpGet    = "GET"
	OpSet    = "SET"
	OpSetEx  = "SETEX"
	OpDelete = "DELETE"
	OpList   = "LIST"
)

// Request is the argument of the Distkv.Execute RPC.
type Request struct {
	Op    string
	Key   string
	Value string
	TTL   time.Duration // SETEX only
}

// Response is the reply of the Distkv.Execute RPC.
type Response struct {
	// Served is true when this node handled the request. When false the
	// node is not the leader and Leader holds a redirect hint (possibly
	// empty if no leader is currently known).
	Served bool
	Leader string

	Value string   // GET
	Found bool     // GET
	Keys  []string // LIST
}

// kv is the replicated key-value state machine. It implements
// raft.StateMachine: every mutation arrives through Apply, exactly once and in
// log order, which is what keeps the replicas byte-for-byte identical.
type kv struct {
	mu     sync.RWMutex
	data   map[string]string
	expiry map[string]int64 // key -> absolute expiry, Unix nanoseconds
}

func newKV() *kv {
	return &kv{
		data:   make(map[string]string),
		expiry: make(map[string]int64),
	}
}

// Apply executes a committed command. It is the sole mutation path for the
// store, so the switch here is the complete write API. Reads are intentionally
// absent: GET and LIST never reach the log.
func (s *kv) Apply(cmd raft.Command) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case raft.OpSet:
		s.data[cmd.Key] = cmd.Value
		delete(s.expiry, cmd.Key)
		return "OK"
	case raft.OpSetEx:
		s.data[cmd.Key] = cmd.Value
		s.expiry[cmd.Key] = cmd.ExpireAt
		return "OK"
	case raft.OpDelete:
		delete(s.data, cmd.Key)
		delete(s.expiry, cmd.Key)
		return "OK"
	default:
		return "OK"
	}
}

// get returns a value, treating an elapsed TTL as absence. This lazy check
// keeps reads correct in the window before the reaper sweeps an expired key.
func (s *kv) get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if expiresAt, ok := s.expiry[key]; ok && time.Now().UnixNano() >= expiresAt {
		return "", false
	}
	value, ok := s.data[key]
	return value, ok
}

// list returns, in sorted order, every live key sharing the given prefix.
func (s *kv) list(prefix string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UnixNano()
	var keys []string
	for key := range s.data {
		if expiresAt, ok := s.expiry[key]; ok && now >= expiresAt {
			continue
		}
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// reapExpired evicts keys whose TTL has elapsed. Because every node stamps the
// same absolute ExpireAt into the log, the reaper deletes the same keys on
// every node without any extra coordination.
func (s *kv) reapExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixNano()
	for key, expiresAt := range s.expiry {
		if now >= expiresAt {
			delete(s.data, key)
			delete(s.expiry, key)
		}
	}
}

// Distkv is the client-facing coordinator for one cluster node. It owns the
// Raft node and the state machine, routes writes through consensus, and serves
// reads locally.
type Distkv struct {
	node   *raft.Node
	kv     *kv
	ttl    *ttlReaper
	stream *streamPublisher // nil when StreamQ publishing is disabled
}

// NewDistkv assembles a node: the key-value state machine, the Raft node that
// replicates it, and the TTL reaper. When streamqAddr is non-empty, committed
// writes are also published to a StreamQ broker at that address for real-time
// event propagation; an empty address disables publishing entirely.
func NewDistkv(id int, cluster map[int]string, dataDir, streamqAddr string) (*Distkv, error) {
	state := newKV()
	node, err := raft.NewNode(id, cluster, state, dataDir)
	if err != nil {
		return nil, err
	}
	h := &Distkv{node: node, kv: state, ttl: newTTLReaper(state)}

	if streamqAddr != "" {
		// Entries already in the log at startup were replayed from the
		// write-ahead log and belong to an earlier process lifetime; they
		// were published then, if at all. Only entries appended above this
		// index are writes newly committed in this lifetime.
		restoredIndex := node.LastIndex()
		h.stream = newStreamPublisher(streamqAddr)

		// Publish each committed write once for the whole cluster: the
		// observer runs on every replica in log order, but only the leader
		// forwards the event, and replayed history is skipped.
		node.SetApplyObserver(func(entry raft.LogEntry) {
			if entry.Index <= restoredIndex || !node.IsLeader() {
				return
			}
			h.stream.enqueue(streamEvent{
				op:    entry.Command.Op,
				key:   entry.Command.Key,
				value: entry.Command.Value,
				term:  uint64(entry.Term),
			})
		})
	}
	return h, nil
}

// Start brings the node online.
func (h *Distkv) Start() {
	if h.stream != nil {
		h.stream.start()
	}
	h.node.Start()
	h.ttl.start()
}

// Stop takes the node offline. It is idempotent.
func (h *Distkv) Stop() {
	h.ttl.stop()
	h.node.Stop()
	if h.stream != nil {
		h.stream.stop()
	}
}

// Node exposes the underlying Raft node, primarily for observability.
func (h *Distkv) Node() *raft.Node { return h.node }

// IsLeader reports whether this node currently leads the cluster.
func (h *Distkv) IsLeader() bool { return h.node.IsLeader() }

// Register publishes both RPC services this node serves: the internal "Raft"
// service used between peers and the client-facing "Distkv" service.
func (h *Distkv) Register(srv *rpc.Server) error {
	if err := h.node.Register(srv); err != nil {
		return err
	}
	return srv.RegisterName("Distkv", &rpcService{distkv: h})
}

// rpcService adapts Distkv to the net/rpc method convention.
type rpcService struct {
	distkv *Distkv
}

// Execute is the single client-facing RPC entry point.
func (s *rpcService) Execute(req Request, resp *Response) error {
	return s.distkv.execute(req, resp)
}

// execute dispatches one client request. Writes are proposed through Raft;
// reads are answered from the local replica, but only by the leader, so that a
// follower never serves data it has not yet had confirmed. (A stale leader can
// still briefly serve a slightly old read; full read linearizability would add
// the Raft read-index protocol, which is out of scope here.)
func (h *Distkv) execute(req Request, resp *Response) error {
	switch req.Op {
	case OpGet, OpList:
		// LeaderReady is false both when this node is not the leader and
		// when it is a freshly elected leader whose state machine has not
		// yet replayed its log. Either way the client is redirected or
		// retries, so a read never observes a lagging replica.
		if !h.node.LeaderReady() {
			resp.Leader = h.node.LeaderAddr()
			return nil
		}
		if req.Op == OpGet {
			resp.Value, resp.Found = h.kv.get(req.Key)
		} else {
			resp.Keys = h.kv.list(req.Key)
		}
		resp.Served = true
		return nil

	case OpSet, OpDelete, OpSetEx:
		cmd := raft.Command{Op: req.Op, Key: req.Key, Value: req.Value}
		if req.Op == OpSetEx {
			// The leader stamps the absolute expiry once so that every
			// replica expires the key at the identical instant.
			cmd.ExpireAt = time.Now().Add(req.TTL).UnixNano()
		}
		value, err := h.node.Propose(cmd)
		if err != nil {
			var notLeader *raft.NotLeaderError
			if errors.As(err, &notLeader) {
				resp.Leader = notLeader.LeaderAddr
				return nil
			}
			return err
		}
		resp.Value = value
		resp.Served = true
		return nil

	default:
		return fmt.Errorf("distkv: unknown operation %q", req.Op)
	}
}
