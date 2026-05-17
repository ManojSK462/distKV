package test

import (
	"testing"
	"time"
)

// After the leader is killed, a write made beforehand must still be readable
// from the cluster under its new leader.
func TestFaultDataSurvivesLeaderFailure(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	leader := tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	if err := c.Set("config::feature_flags::dark_mode", "true"); err != nil {
		t.Fatalf("write before failure: %v", err)
	}

	tc.stop(leader)
	newLeader := tc.requireLeader(3 * time.Second)
	if newLeader == leader {
		t.Fatalf("stopped node %d is still leading", leader)
	}

	value, found, err := c.Get("config::feature_flags::dark_mode")
	if err != nil || !found {
		t.Fatalf("read after failover = (found=%v, err=%v); want the value", found, err)
	}
	if value != "true" {
		t.Fatalf("read after failover = %q; want \"true\"", value)
	}
}

// A three-node cluster tolerates one node being down: it keeps a leader and
// keeps serving reads and writes.
func TestFaultClusterToleratesOneNodeDown(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	leader := tc.requireLeader(3 * time.Second)

	tc.stop(leader) // run with only two of three nodes
	tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	if err := c.Set("config::k", "v"); err != nil {
		t.Fatalf("write with one node down: %v", err)
	}
	value, found, err := c.Get("config::k")
	if err != nil || !found || value != "v" {
		t.Fatalf("read with one node down = (%q, found=%v, err=%v); want (\"v\", true, nil)", value, found, err)
	}
}

// Every node is stopped and restarted; all committed data must come back,
// rebuilt from the persisted logs.
func TestFaultClusterRestartRestoresData(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	tc.requireLeader(3 * time.Second)

	c := tc.client()
	if err := c.Set("config::db::connection_timeout_ms", "5000"); err != nil {
		t.Fatalf("write before restart: %v", err)
	}
	if err := c.SetConfig("rate_limits::api_calls_per_min", "1000"); err != nil {
		t.Fatalf("write before restart: %v", err)
	}
	c.Close()

	ids := []int{1, 2, 3}
	for _, id := range ids {
		tc.stop(id)
	}
	for _, id := range ids {
		tc.restart(id)
	}
	tc.requireLeader(5 * time.Second)

	c2 := tc.client()
	defer c2.Close()

	value, found, err := c2.Get("config::db::connection_timeout_ms")
	if err != nil || !found || value != "5000" {
		t.Fatalf("restored read = (%q, found=%v, err=%v); want (\"5000\", true, nil)", value, found, err)
	}
	if v, found, _ := c2.GetConfig("rate_limits::api_calls_per_min"); !found || v != "1000" {
		t.Fatalf("restored config read = (%q, found=%v); want (\"1000\", true)", v, found)
	}
}
