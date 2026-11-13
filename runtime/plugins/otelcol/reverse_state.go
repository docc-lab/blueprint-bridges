package otelcol

// The per-span response-path state table, for spans of processors that keep none in the wire table.
//
// Every key is new, stored once, loaded once and deleted once. sync.Map suits read-mostly maps
// with a stable key set and does badly here: a new key always takes its slow path, and its
// periodic promotions copy every live entry. Fixed mutex-guarded shards avoid both: span IDs are
// random, so load spreads evenly, and each lock is held for one map operation.

import (
	"sync"

	"go.opentelemetry.io/otel/trace"
)

// reverseStateShards is a power of two so the shard index is a mask.
const reverseStateShards = 64

type reverseStateShard struct {
	mu sync.Mutex
	m  map[trace.SpanID]*reverseSpanState
	_  [64 - (8+8)%64]byte // keep neighbouring shard locks off one cache line
}

type reverseStateTable struct {
	shards [reverseStateShards]reverseStateShard
}

func newReverseStateTable() *reverseStateTable {
	t := &reverseStateTable{}
	for i := range t.shards {
		t.shards[i].m = make(map[trace.SpanID]*reverseSpanState)
	}
	return t
}

// shard folds all eight bytes of the ID, so the balance does not depend on where an ID generator
// puts its randomness.
func (t *reverseStateTable) shard(id trace.SpanID) *reverseStateShard {
	h := id[0] ^ id[1] ^ id[2] ^ id[3] ^ id[4] ^ id[5] ^ id[6] ^ id[7]
	return &t.shards[h&(reverseStateShards-1)]
}

func (t *reverseStateTable) store(id trace.SpanID, state *reverseSpanState) {
	s := t.shard(id)
	s.mu.Lock()
	s.m[id] = state
	s.mu.Unlock()
}

func (t *reverseStateTable) load(id trace.SpanID) *reverseSpanState {
	s := t.shard(id)
	s.mu.Lock()
	state := s.m[id]
	s.mu.Unlock()
	return state
}

func (t *reverseStateTable) delete(id trace.SpanID) {
	s := t.shard(id)
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}
