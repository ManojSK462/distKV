package test

import (
	"testing"
	"time"
)

// A fresh cluster must converge on exactly one leader.
func TestElectionProducesSingleLeader(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()

	tc.requireLeader(3 * time.Second)
}

// When the leader disappears the survivors must elect a new one, promptly,
// without electing two.
func TestElectionRecoversAfterLeaderLoss(t *testing.T) {
	tc := newCluster(t, 3)
	defer tc.stopAll()

	first := tc.requireLeader(3 * time.Second)
	tc.stop(first)

	start := time.Now()
	second, ok := tc.waitForLeader(3 * time.Second)
	if !ok {
		t.Fatal("no new leader was elected after the leader was stopped")
	}
	if second == first {
		t.Fatalf("stopped node %d is still reported as leader", first)
	}
	t.Logf("re-elected in %s (new leader: node %d)", time.Since(start).Round(time.Millisecond), second)
}
