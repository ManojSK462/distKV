package test

import (
	"testing"
	"time"
)

// A session written with a TTL must be readable immediately and gone once the
// TTL has elapsed.
func TestSessionTTLExpires(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	if err := c.SetSession("user_123", "token-abc", time.Second); err != nil {
		t.Fatalf("writing session: %v", err)
	}
	if _, found, _ := c.GetSession("user_123"); !found {
		t.Fatal("session is missing immediately after it was written")
	}

	time.Sleep(2500 * time.Millisecond)

	if _, found, _ := c.GetSession("user_123"); found {
		t.Fatal("session is still present after its TTL elapsed")
	}
}

// A session must remain readable through a leader failover.
func TestSessionSurvivesFailover(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()
	leader := tc.requireLeader(3 * time.Second)

	c := tc.client()
	defer c.Close()

	if err := c.SetSession("user_456", "token-xyz", time.Hour); err != nil {
		t.Fatalf("writing session: %v", err)
	}

	tc.stop(leader)
	tc.requireLeader(3 * time.Second)

	value, found, err := c.GetSession("user_456")
	if err != nil {
		t.Fatalf("reading session after failover: %v", err)
	}
	if !found || value != "token-xyz" {
		t.Fatalf("session after failover = (%q, found=%v); want (\"token-xyz\", true)", value, found)
	}
}
