package test

import (
	"testing"
	"time"
)

// A value written through the leader must be readable afterwards.
func TestReplicationWriteThenRead(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	if err := c.SetConfig("feature_flags::dark_mode", "true"); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	value, found, err := c.GetConfig("feature_flags::dark_mode")
	if err != nil {
		t.Fatalf("reading config: %v", err)
	}
	if !found || value != "true" {
		t.Fatalf("config read = (%q, found=%v); want (\"true\", found=true)", value, found)
	}
}

// LIST must return every key under a prefix and nothing outside it.
func TestReplicationListByPrefix(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	if err := c.SetConfig("rate_limits::api", "1000"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.SetConfig("rate_limits::login", "10"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := c.SetSession("user_1", "token", time.Minute); err != nil {
		t.Fatalf("write: %v", err)
	}

	keys, err := c.ListConfig()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("ListConfig returned %d keys (%v); want 2", len(keys), keys)
	}
}

// A read served by the cluster must reflect the most recent write to a key.
func TestReplicationOverwrite(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	const key = "config::db::timeout_ms"
	if err := c.Set(key, "5000"); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := c.Set(key, "8000"); err != nil {
		t.Fatalf("second write: %v", err)
	}

	value, found, err := c.Get(key)
	if err != nil || !found {
		t.Fatalf("get = (found=%v, err=%v); want a value", found, err)
	}
	if value != "8000" {
		t.Fatalf("get = %q; want the overwritten value \"8000\"", value)
	}
}
