package store

import (
	"log"
	"sync"
	"time"

	streamqbridge "streamq/integration/distkv"
)

// streamBufferSize bounds how many committed writes may await publication
// before the oldest are dropped. It absorbs a write burst or a brief broker
// stall without ever applying back-pressure to consensus.
const streamBufferSize = 1024

// streamEvent is one committed write awaiting publication to StreamQ.
type streamEvent struct {
	op    string
	key   string
	value string
	term  uint64
}

// streamPublisher forwards committed writes to a StreamQ broker. Propagation
// is strictly best-effort: it never blocks consensus and never fails a write.
// Events are buffered and drained by a single goroutine, so they reach the
// broker in log order; the broker connection is established lazily and
// re-established after any failure, so the broker may start, stop, or restart
// independently of the distKV cluster.
type streamPublisher struct {
	brokerAddr string
	events     chan streamEvent
	stopCh     chan struct{}
	done       chan struct{}
	once       sync.Once
}

func newStreamPublisher(brokerAddr string) *streamPublisher {
	return &streamPublisher{
		brokerAddr: brokerAddr,
		events:     make(chan streamEvent, streamBufferSize),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// enqueue hands an event to the drain goroutine without ever blocking. If the
// buffer is full the event is dropped — propagation is best-effort, and the
// Raft log remains the authoritative record of every write.
func (p *streamPublisher) enqueue(ev streamEvent) {
	select {
	case p.events <- ev:
	default:
		log.Printf("distkv: streamq buffer full, dropping event for key %q", ev.key)
	}
}

// start launches the drain goroutine.
func (p *streamPublisher) start() {
	go p.run()
}

// stop halts the drain goroutine. It waits briefly for an in-flight publish to
// settle but will not let an unresponsive broker stall shutdown.
func (p *streamPublisher) stop() {
	p.once.Do(func() { close(p.stopCh) })
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
	}
}

// run drains buffered events to the broker in order. It owns the broker
// connection: it dials on the first event and redials after any failure, so a
// broker that is down at startup or restarts later is tolerated transparently.
func (p *streamPublisher) run() {
	defer close(p.done)

	var pub *streamqbridge.Publisher
	defer func() {
		if pub != nil {
			pub.Close()
		}
	}()

	for {
		select {
		case <-p.stopCh:
			return
		case ev := <-p.events:
			if pub == nil {
				connected, err := streamqbridge.NewPublisher(p.brokerAddr)
				if err != nil {
					log.Printf("distkv: streamq broker %s unreachable (%v); dropping event for key %q",
						p.brokerAddr, err, ev.key)
					continue
				}
				pub = connected
			}
			if _, err := pub.Publish(streamqbridge.CommandEvent{
				Op:    ev.op,
				Key:   ev.key,
				Value: ev.value,
				Term:  ev.term,
			}); err != nil {
				log.Printf("distkv: streamq publish failed (%v); will reconnect", err)
				pub.Close()
				pub = nil
			}
		}
	}
}
