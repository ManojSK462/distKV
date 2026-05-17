// Package test exercises a Distkv cluster end to end. The harness here starts
// real nodes communicating over real TCP loopback connections, so the tests
// cover the genuine RPC, election, and replication paths rather than mocks.
package test

import (
	"fmt"
	"net"
	"net/rpc"
	"path/filepath"
	"testing"
	"time"

	"distkv/client"
	"distkv/store"
)

// testNode is one running node together with the resources it owns.
type testNode struct {
	id       int
	distkv   *store.Distkv
	listener net.Listener
}

// testCluster manages a set of nodes for a single test.
type testCluster struct {
	t       *testing.T
	cluster map[int]string // id -> address, fixed for the cluster's lifetime
	dirs    map[int]string // id -> persistent state directory
	nodes   map[int]*testNode
}

// newCluster starts a cluster of the given size. A free loopback port is
// reserved for every node before any node is constructed, so the full cluster
// map is known up front and survives restarts.
func newCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	tc := &testCluster{
		t:       t,
		cluster: make(map[int]string),
		dirs:    make(map[int]string),
		nodes:   make(map[int]*testNode),
	}

	reserved := make(map[int]net.Listener)
	for id := 1; id <= size; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserving a port for node %d: %v", id, err)
		}
		reserved[id] = ln
		tc.cluster[id] = ln.Addr().String()
		tc.dirs[id] = filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", id))
	}
	for id := 1; id <= size; id++ {
		tc.startNode(id, reserved[id])
	}
	return tc
}

// startNode brings up node id, serving on the supplied listener.
func (tc *testCluster) startNode(id int, listener net.Listener) {
	tc.t.Helper()

	distkv, err := store.NewDistkv(id, tc.cluster, tc.dirs[id], "")
	if err != nil {
		tc.t.Fatalf("creating node %d: %v", id, err)
	}
	server := rpc.NewServer()
	if err := distkv.Register(server); err != nil {
		tc.t.Fatalf("registering node %d: %v", id, err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	distkv.Start()
	tc.nodes[id] = &testNode{id: id, distkv: distkv, listener: listener}
}

// stop shuts node id down. Its persistent state is left on disk so the node
// can later be restarted.
func (tc *testCluster) stop(id int) {
	node := tc.nodes[id]
	if node == nil {
		return
	}
	node.listener.Close()
	node.distkv.Stop()
	delete(tc.nodes, id)
}

// stopAll shuts down every running node.
func (tc *testCluster) stopAll() {
	for id := range tc.nodes {
		tc.stop(id)
	}
}

// restart brings a previously stopped node back up on its original address,
// reusing its on-disk state.
func (tc *testCluster) restart(id int) {
	tc.t.Helper()
	listener := tc.relisten(id)
	tc.startNode(id, listener)
}

// relisten rebinds a node's address, retrying briefly because the port may not
// be immediately reusable right after the previous listener closed.
func (tc *testCluster) relisten(id int) net.Listener {
	tc.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 50; attempt++ {
		ln, err := net.Listen("tcp", tc.cluster[id])
		if err == nil {
			return ln
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("rebinding node %d on %s: %v", id, tc.cluster[id], lastErr)
	return nil
}

// waitForLeader blocks until exactly one node reports leadership, returning
// that node's id. The boolean is false if the timeout elapses first.
func (tc *testCluster) waitForLeader(timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []int
		for id, node := range tc.nodes {
			if node.distkv.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0], true
		}
		time.Sleep(15 * time.Millisecond)
	}
	return 0, false
}

// requireLeader fails the test unless a single leader emerges in time.
func (tc *testCluster) requireLeader(timeout time.Duration) int {
	tc.t.Helper()
	leader, ok := tc.waitForLeader(timeout)
	if !ok {
		tc.t.Fatal("cluster did not converge on a single leader")
	}
	return leader
}

// client returns a client wired to every node currently in the cluster map.
func (tc *testCluster) client() *client.Client {
	addrs := make([]string, 0, len(tc.cluster))
	for _, addr := range tc.cluster {
		addrs = append(addrs, addr)
	}
	return client.New(addrs)
}
