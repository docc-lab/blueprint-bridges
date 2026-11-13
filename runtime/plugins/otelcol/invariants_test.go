package otelcol

// Each invariant the processors check panics with a *backend.InvariantError when broken, and what a
// real parent span hands its child passes.

import (
	"context"
	"testing"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mustBreakInvariant fails the test unless f panics with a *backend.InvariantError.
func mustBreakInvariant(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if _, ok := recover().(*backend.InvariantError); !ok {
			t.Errorf("%s: no tracing invariant panic", name)
		}
	}()
	f()
}

// childOf is the context a child span of parent starts with: what the instrumentation puts there
// (the parent's outgoing `_br` as baggage, the child ordinal), with br in place of the real `_br`
// when it is not "".
func childOf(parent trace.Span, br string, seqNum int) context.Context {
	if br == "" {
		br = baggageBR(parent)
	}
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent.SpanContext())
	if seqNum != 0 {
		ctx = context.WithValue(ctx, "seqNum", seqNum)
	}
	return backend.SetBaggageInContext(ctx, map[string]string{BaggageBRKey: br})
}

func TestInvariantParentWindow(t *testing.T) {
	t.Setenv("REVERSE_TRUSS", "off")
	for _, kind := range []string{"pb", "cgpb", "sb"} {
		tp, _ := ttlTestProvider(t, kind, checkpointRange{2, 6})
		tr := tp.Tracer("inv")
		_, root := tr.Start(context.Background(), "root", trace.WithSpanKind(trace.SpanKindServer))
		// A real child of the root passes.
		_, child := tr.Start(childOf(root, "", 1), "child", trace.WithSpanKind(trace.SpanKindClient))
		child.End()

		real := []byte(baggageBR(root))
		mustBreakInvariant(t, kind+": `_br` that does not unpack", func() {
			tr.Start(childOf(root, string(real[:2]), 1), "bad")
		})
		ttl := append([]byte(nil), real...)
		ttl[0] = 255 // the TTL byte, far above any distance this range draws
		mustBreakInvariant(t, kind+": TTL not below the distance", func() {
			tr.Start(childOf(root, string(ttl), 1), "bad")
		})
		if kind != "pb" {
			mustBreakInvariant(t, kind+": child ordinal missing", func() {
				tr.Start(childOf(root, "", 0), "bad")
			})
		}
		root.End()
	}
}

func TestInvariantPBWindowHasNoHA(t *testing.T) {
	t.Setenv("REVERSE_TRUSS", "off")
	tp, _ := ttlTestProvider(t, "pb", checkpointRange{2, 6})
	tr := tp.Tracer("inv")
	_, root := tr.Start(context.Background(), "root", trace.WithSpanKind(trace.SpanKindServer))
	withHA := baggageBR(root) + string(make([]byte, 9)) // a CGPB-style HA entry after the Bloom
	mustBreakInvariant(t, "PB window with HA", func() { tr.Start(childOf(root, withHA, 1), "bad") })
	root.End()
}

func TestInvariantSBRemEndEvents(t *testing.T) {
	t.Setenv("REVERSE_TRUSS", "off")
	tp, _ := ttlTestProvider(t, "sb", checkpointRange{2, 6})
	_, span := tp.Tracer("inv").Start(context.Background(), "server", trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(attribute.String("remEndEvents", string([]byte{0xff}))) // a varint that never ends
	mustBreakInvariant(t, "remEndEvents that does not decode", func() { span.End() })
}

func TestInvariantWireTableStoreOnce(t *testing.T) {
	table := newWireTable()
	id := trace.SpanID{1, 2, 3}
	table.store(id, &bridgeWire{})
	mustBreakInvariant(t, "second store", func() { table.store(id, &bridgeWire{}) })
	if table.take(id) == nil || table.take(id) != nil {
		t.Error("take is not take-once")
	}
}

func TestInvariantWindowAndEndGroupBounds(t *testing.T) {
	mustBreakInvariant(t, "distance 0", func() {
		encodeWindowPayloads(checkpointRange{2, 6}, 1, [8]byte{}, 0, nil, nil, false, 1, trace.SpanID{1})
	})
	mustBreakInvariant(t, "distance 257", func() {
		encodeWindowPayloads(checkpointRange{2, 6}, 1, [8]byte{}, 257, nil, nil, false, 1, trace.SpanID{1})
	})
	packEndGroup(nil, []int{3, 1, 2}, 2) // an ordinal above the bound is legal (a later sibling ended first)
	mustBreakInvariant(t, "end event 0", func() { packEndGroup(nil, []int{2, 0}, 3) })
}

func TestInvariantExportFraming(t *testing.T) {
	mustBreakInvariant(t, "empty batch", func() { buildBatch([]byte{1}, nil, nil) })
	mustBreakInvariant(t, "batches of 0", func() { batches([]byte{1}, nil, 0, func(*wireBatch) {}) })
	b := buildBatch([]byte{1}, nil, []wireSpan{{body: []byte{1}}})
	b.release()
	mustBreakInvariant(t, "second release", func() { b.release() })
	mustBreakInvariant(t, "reverse context on an ordinary span", func() {
		appendBridgeWireBytes(nil, bridgeBytes{rc: "x", depth: []byte{1}}, false)
	})
	mustBreakInvariant(t, "field 16", func() { closeMessage([]byte{1}, 16, 0) })
	if got := closeMessage([]byte{7}, 15, 0); len(got) != 3 || got[0] != 15<<3|2 || got[1] != 1 {
		t.Errorf("closeMessage(15) = %v", got)
	}
}

func TestInvariantPrepareChildren(t *testing.T) {
	p := &reverseCheckpointProcessor{}
	noop := trace.SpanFromContext(context.Background())
	if got := p.prepare(noop, "", -1); got != "" { // unknown count, a non-recording span: nothing to do
		t.Errorf("prepare = %q", got)
	}
	mustBreakInvariant(t, "child count -2", func() { p.prepare(noop, "", -2) })
}
