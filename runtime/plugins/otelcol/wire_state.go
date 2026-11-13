package otelcol

// Per-span bridge state, kept in a table beside the span rather than as span attributes.
//
// Attributes are expensive here: every read dedupes the span's whole attribute set under a lock,
// and values must be strings. Only the outgoing `_br` baggage has to reach other services; the rest
// is read by this process alone. So OnStart stores the state once as raw bytes, the response path
// reads it, and OnEnd takes (and frees) it.

import (
	"sync"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/trace"
)

// bridgeWire is one span's bridge state that never has to be on the OTel span.
type bridgeWire struct {
	emit     []byte // exported `_br` payload (checkpoints and leaves)
	depth    int    // absolute depth, exported as `_d` on interior non-checkpoints
	priority bool   // scheduled checkpoint
	prop     string // outgoing `_br` baggage value
	// SB only: non-checkpoints also export `_o`, the span's child ordinal.
	structural bool
	ordinal    int
	// rev is the response path's state for this span (reverse_checkpoint.go).
	rev reverseSpanState
}

// bridgeWires is process-wide: one bridge processor per service, and span IDs are unique.
var bridgeWires = newWireTable()

// The instrumentation adds a bridge span's outgoing `_br` to its baggage from this table
// (backend.AppendSpanBaggage).
func init() {
	backend.RegisterSpanBaggageSource(func(sc trace.SpanContext, baggage map[string]string) {
		if w := bridgeWires.load(sc.SpanID()); w != nil {
			baggage[BaggageBRKey] = w.prop
		}
	})
}

// wireTable is a sharded map from span ID to state that is stored once, read, and taken once.
// It is not generic because Blueprint's compiler parses this package with a parser that does not
// accept type parameters.
const wireTableShards = 64

type wireTableShard struct {
	mu sync.Mutex
	m  map[trace.SpanID]*bridgeWire
	_  [64 - (8+8)%64]byte // keep neighbouring shard locks off one cache line
}

type wireTable struct {
	shards [wireTableShards]wireTableShard
}

func newWireTable() *wireTable {
	t := &wireTable{}
	for i := range t.shards {
		t.shards[i].m = make(map[trace.SpanID]*bridgeWire)
	}
	return t
}

func (t *wireTable) shard(id trace.SpanID) *wireTableShard {
	h := id[0] ^ id[1] ^ id[2] ^ id[3] ^ id[4] ^ id[5] ^ id[6] ^ id[7]
	return &t.shards[h&(wireTableShards-1)]
}

func (t *wireTable) store(id trace.SpanID, v *bridgeWire) {
	s := t.shard(id)
	s.mu.Lock()
	if _, dup := s.m[id]; dup {
		s.mu.Unlock()
		brokenRule("a span's wire entry is stored once")
	}
	s.m[id] = v
	s.mu.Unlock()
}

func (t *wireTable) load(id trace.SpanID) *bridgeWire {
	s := t.shard(id)
	s.mu.Lock()
	v := s.m[id]
	s.mu.Unlock()
	return v
}

// take loads and deletes the entry.
func (t *wireTable) take(id trace.SpanID) *bridgeWire {
	s := t.shard(id)
	s.mu.Lock()
	v := s.m[id]
	delete(s.m, id)
	s.mu.Unlock()
	return v
}
