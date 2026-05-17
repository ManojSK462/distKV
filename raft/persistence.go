package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// persistentState is the subset of Raft state that must outlive a crash: the
// current term, the vote cast in that term, and the log itself. commitIndex
// and lastApplied are deliberately excluded — they are volatile and are
// rediscovered after a restart once a leader emerges and commits a no-op.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// persister durably stores persistentState for a single node.
type persister struct {
	path string
}

func newPersister(dataDir string, nodeID int) (*persister, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: creating data directory: %w", err)
	}
	name := fmt.Sprintf("node-%d.state", nodeID)
	return &persister{path: filepath.Join(dataDir, name)}, nil
}

// save writes the state atomically: it is encoded to a temporary file and then
// renamed over the live file. A reader — including this node after a crash
// mid-write — therefore always observes a complete previous or new state,
// never a torn one.
func (p *persister) save(state persistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}

// load reads previously saved state. The second result is false when no state
// file exists yet, i.e. this is the node's first run.
func (p *persister) load() (persistentState, bool, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return persistentState{}, false, nil
	}
	if err != nil {
		return persistentState{}, false, err
	}
	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistentState{}, false, fmt.Errorf("raft: corrupt state file %s: %w", p.path, err)
	}
	return state, true, nil
}

// persistLocked flushes durable state. It must be called with n.mu held, and
// must be called on every change to currentTerm, votedFor, or the log before
// that change is acted upon or acknowledged to a peer.
func (n *Node) persistLocked() {
	err := n.persister.save(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	})
	if err != nil {
		log.Printf("raft: node %d: could not persist state: %v", n.id, err)
	}
}
