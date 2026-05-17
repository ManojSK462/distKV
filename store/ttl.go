package store

import (
	"sync"
	"time"
)

// reapInterval is how often expired keys are actively swept. Reads expire keys
// lazily in between, so this interval bounds memory, not correctness.
const reapInterval = time.Second

// ttlReaper periodically evicts keys whose session TTL has elapsed.
type ttlReaper struct {
	kv     *kv
	stopCh chan struct{}
	once   sync.Once
}

func newTTLReaper(kv *kv) *ttlReaper {
	return &ttlReaper{kv: kv, stopCh: make(chan struct{})}
}

// start launches the background sweep loop.
func (r *ttlReaper) start() {
	go func() {
		ticker := time.NewTicker(reapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				return
			case <-ticker.C:
				r.kv.reapExpired()
			}
		}
	}()
}

// stop halts the sweep loop. It is safe to call more than once.
func (r *ttlReaper) stop() {
	r.once.Do(func() { close(r.stopCh) })
}
