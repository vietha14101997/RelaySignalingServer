package telemetry

import (
	"sync"
	"time"
)

// ringCapacity bounds the recent-snapshot admin diagnostics buffer. Oldest
// entries are evicted (overwritten) once full — this is the ONLY place a
// per-report identifier (session_id) is retained, and only for admin reads.
const ringCapacity = 500

// recentSnapshot is one admin-visible raw snapshot: the normalized report
// (embedded, flattens its json tags) plus server-side receive time.
type recentSnapshot struct {
	report
	ReceivedAt time.Time `json:"received_at"`
}

// aggregator holds bounded in-memory counters keyed by low-cardinality enums,
// plus the bounded recent-snapshot ring. NO Postgres/persistence (Phase 00 —
// deferred to Phase 06) and NO unbounded per-session map.
type aggregator struct {
	mu    sync.Mutex
	total int64

	pathClass     map[string]int64
	family        map[string]int64
	protocol      map[string]int64
	relayProtocol map[string]int64
	pcRole        map[string]int64
	source        map[string]int64
	codec         map[string]int64
	event         map[string]int64

	ring     []recentSnapshot // circular buffer, capacity ringCapacity
	ringNext int              // next write index once ring is full
}

func newAggregator() *aggregator {
	return &aggregator{
		pathClass:     make(map[string]int64),
		family:        make(map[string]int64),
		protocol:      make(map[string]int64),
		relayProtocol: make(map[string]int64),
		pcRole:        make(map[string]int64),
		source:        make(map[string]int64),
		codec:         make(map[string]int64),
		event:         make(map[string]int64),
		ring:          make([]recentSnapshot, 0, ringCapacity),
	}
}

// record folds one normalized report into the bounded counters and ring.
func (a *aggregator) record(r report) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.total++
	a.pathClass[r.PathClass]++
	a.family[r.AddressFamily]++
	a.protocol[r.Protocol]++
	a.relayProtocol[r.RelayProtocol]++
	a.pcRole[r.PCRole]++
	a.source[r.Source]++
	a.codec[r.Codec]++
	a.event[r.Event]++

	entry := recentSnapshot{report: r, ReceivedAt: time.Now().UTC()}
	if len(a.ring) < ringCapacity {
		a.ring = append(a.ring, entry)
		return
	}
	// Ring full: overwrite oldest slot (circular eviction).
	a.ring[a.ringNext] = entry
	a.ringNext = (a.ringNext + 1) % ringCapacity
}

// statsData is a fully-copied, lock-free snapshot of aggregator state for
// building the /telemetry/stats response.
type statsData struct {
	total         int64
	pathClass     map[string]int64
	family        map[string]int64
	protocol      map[string]int64
	relayProtocol map[string]int64
	pcRole        map[string]int64
	source        map[string]int64
	codec         map[string]int64
	event         map[string]int64
	recent        []recentSnapshot // newest first
}

func copyCounts(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// snapshot returns a point-in-time copy of all aggregate state under a single
// lock (avoids torn reads across counters + ring).
func (a *aggregator) snapshot() statsData {
	a.mu.Lock()
	defer a.mu.Unlock()

	data := statsData{
		total:         a.total,
		pathClass:     copyCounts(a.pathClass),
		family:        copyCounts(a.family),
		protocol:      copyCounts(a.protocol),
		relayProtocol: copyCounts(a.relayProtocol),
		pcRole:        copyCounts(a.pcRole),
		source:        copyCounts(a.source),
		codec:         copyCounts(a.codec),
		event:         copyCounts(a.event),
	}

	// Reconstruct chronological (oldest-first) order from the circular
	// buffer, then reverse to newest-first for admin readability.
	var chron []recentSnapshot
	if len(a.ring) < ringCapacity {
		chron = make([]recentSnapshot, len(a.ring))
		copy(chron, a.ring)
	} else {
		chron = make([]recentSnapshot, ringCapacity)
		n := copy(chron, a.ring[a.ringNext:])
		copy(chron[n:], a.ring[:a.ringNext])
	}
	recent := make([]recentSnapshot, len(chron))
	for i, e := range chron {
		recent[len(chron)-1-i] = e
	}
	data.recent = recent

	return data
}
