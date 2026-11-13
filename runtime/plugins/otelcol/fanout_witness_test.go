package otelcol

// A truss carries at most one fan-out witness: a span's second child gets a new one, a first child
// inherits its parent's, later children get none, and a checkpoint opens its window without one.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// randomWitness returns no fan-out witness, or one recorded at a random depth from 1 to depth.
func randomWitness(r *rand.Rand, depth int) []byte {
	if depth < 1 || r.Intn(2) == 0 {
		return nil
	}
	var id trace.SpanID
	r.Read(id[:])
	return newFanoutWitness(id, 1+r.Intn(depth))
}

// witnessMode is a bridge that records fan-out witnesses, with random or fixed checkpoint distances.
type witnessMode struct {
	kind   string
	ranged bool
}

var witnessModes = []witnessMode{{"cgpb", true}, {"cgpb", false}, {"sb", true}, {"sb", false}}

func (m witnessMode) String() string {
	if m.ranged {
		return m.kind + "/range"
	}
	return m.kind + "/fixed"
}

// tracer returns a tracer whose checkpoints are distance hops apart.
func (m witnessMode) tracer(t *testing.T, distance int) trace.Tracer {
	t.Helper()
	if m.ranged {
		tp, _ := ttlTestProvider(t, m.kind, checkpointRange{distance, distance})
		return tp.Tracer("witness")
	}
	oldM, oldK := BloomFilterM, BloomFilterK
	t.Cleanup(func() { BloomFilterM, BloomFilterK = oldM, oldK; backend.SetRTBloomParams(oldM, oldK) })
	setBloomForCPD(distance)
	var p sdktrace.SpanProcessor = &CallGraphBridgeProcessor{checkpointDistance: int64(distance)}
	if m.kind == "sb" {
		p = &StructuralBridgeProcessor{checkpointDistance: int64(distance)}
	}
	return sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(p)).Tracer("witness")
}

// truss returns the truss span exports, or the one it passes to its children, without a TTL byte.
func (m witnessMode) truss(t *testing.T, span trace.Span, exported bool) []byte {
	t.Helper()
	if m.kind == "cgpb" && !m.ranged { // the legacy format keeps both on the span, in base64
		key := attribute.Key(AttrBR)
		if exported {
			key = attribute.Key(AttrBREmit)
		}
		raw, ok := decodeBR(checkpointSpanAttribute(span, key))
		if !ok {
			t.Fatalf("%v: no %s", m, key)
		}
		return raw
	}
	w := bridgeWires.load(span.SpanContext().SpanID())
	if w == nil {
		t.Fatalf("%v: no wire state", m)
	}
	if exported {
		return w.emit
	}
	raw := viewPropBR(w.prop)
	if m.ranged {
		raw = raw[1:]
	}
	return raw
}

// witness returns the fan-out witness in a truss.
func (m witnessMode) witness(t *testing.T, truss []byte) []byte {
	t.Helper()
	var witness []byte
	var ok bool
	switch {
	case m.kind == "sb":
		_, _, _, _, witness, _, ok = unpackStructuralBR(truss)
	case m.ranged:
		_, _, _, _, witness, ok = unpackCheckpointWindowBR(truss)
	default:
		_, _, _, witness, ok = unpackCGPRBBR(truss, int((BloomFilterM+7)/8))
	}
	if !ok {
		t.Fatalf("%v: truss does not unpack", m)
	}
	return witness
}

// withWitness returns the `_br` that span passes to its children, with witness in place of its own.
func (m witnessMode) withWitness(t *testing.T, span trace.Span, witness []byte) string {
	t.Helper()
	truss := m.truss(t, span, false)
	var packed []byte
	switch {
	case m.kind == "sb":
		depth, ckpt, distance, bloomBytes, _, tail, ok := unpackStructuralBR(truss)
		if !ok {
			t.Fatal("truss does not unpack")
		}
		packed = packStructuralBR(depth, ckpt, distance, bloomBytes, witness, tail)
	case m.ranged:
		depth, ckpt, distance, bloomBytes, _, ok := unpackCheckpointWindowBR(truss)
		if !ok {
			t.Fatal("truss does not unpack")
		}
		packed = packCheckpointWindowBR(depth, ckpt, distance, bloomBytes, witness)
	default:
		depth, ckpt, bloomBytes, _, ok := unpackCGPRBBR(truss, int((BloomFilterM+7)/8))
		if !ok {
			t.Fatal("truss does not unpack")
		}
		return encodeBR(packCGPRBBR(depth, ckpt, bloomBytes, witness))
	}
	if m.ranged {
		packed = append([]byte{viewPropBR(baggageBR(span))[0]}, packed...) // the TTL byte
	}
	return propBR(packed)
}

func startChild(tr trace.Tracer, parent trace.Span, seqNum int) trace.Span {
	_, span := tr.Start(childOf(parent, "", seqNum), "child", trace.WithSpanKind(trace.SpanKindServer))
	return span
}

func TestFanoutWitnessRule(t *testing.T) {
	t.Setenv("REVERSE_TRUSS", "off")
	for _, m := range witnessModes {
		t.Run(m.String(), func(t *testing.T) {
			tr := m.tracer(t, 8) // no checkpoint below the root
			_, root := tr.Start(context.Background(), "root", trace.WithSpanKind(trace.SpanKindServer))
			a1, a2, a3 := startChild(tr, root, 1), startChild(tr, root, 2), startChild(tr, root, 3)
			b1, b2, b3 := startChild(tr, a2, 1), startChild(tr, a2, 2), startChild(tr, a2, 3)
			c1, c2 := startChild(tr, b1, 1), startChild(tr, b1, 2)
			d1 := startChild(tr, c1, 1)
			witnessOf := func(span trace.Span, depth int) []byte { return newFanoutWitness(span.SpanContext().SpanID(), depth) }
			for _, tc := range []struct {
				name string
				span trace.Span
				want []byte
			}{
				{"root", root, nil},
				{"first child of a span without a witness", a1, nil},
				{"second child", a2, witnessOf(root, 1)},
				{"third child", a3, nil},
				{"first child inherits", b1, witnessOf(root, 1)},
				{"second child replaces the inherited witness", b2, witnessOf(a2, 2)},
				{"third child drops the inherited witness", b3, nil},
				{"first child of a first child", c1, witnessOf(root, 1)},
				{"second child of a first child", c2, witnessOf(b1, 3)},
				{"first children all the way down", d1, witnessOf(root, 1)},
			} {
				// The span exports the witness it holds, and passes it on to its children unchanged.
				for _, exported := range []bool{false, true} {
					if got := m.witness(t, m.truss(t, tc.span, exported)); !bytes.Equal(got, tc.want) {
						t.Errorf("%s (exported %v): witness %x, want %x", tc.name, exported, got, tc.want)
					}
				}
			}
		})
	}
}

func TestFanoutWitnessEndsAtCheckpoint(t *testing.T) {
	t.Setenv("REVERSE_TRUSS", "off")
	for _, m := range witnessModes {
		t.Run(m.String(), func(t *testing.T) {
			tr := m.tracer(t, 2) // checkpoints at depths 0 and 2
			_, root := tr.Start(context.Background(), "root", trace.WithSpanKind(trace.SpanKindServer))
			a2 := startChild(tr, root, 2)
			checkpoint := startChild(tr, a2, 1)
			want := newFanoutWitness(root.SpanContext().SpanID(), 1)
			if got := m.witness(t, m.truss(t, checkpoint, true)); !bytes.Equal(got, want) {
				t.Errorf("checkpoint exports witness %x, want the one it inherited, %x", got, want)
			}
			if got := m.witness(t, m.truss(t, checkpoint, false)); len(got) != 0 {
				t.Errorf("checkpoint passes on witness %x, want none", got)
			}
			if got := m.witness(t, m.truss(t, startChild(tr, checkpoint, 1), false)); len(got) != 0 {
				t.Errorf("first child below the checkpoint has witness %x, want none", got)
			}
			want = newFanoutWitness(checkpoint.SpanContext().SpanID(), 3)
			if got := m.witness(t, m.truss(t, startChild(tr, checkpoint, 2), false)); !bytes.Equal(got, want) {
				t.Errorf("second child below the checkpoint has witness %x, want %x", got, want)
			}
		})
	}
}

func TestInvariantAtMostOneWitness(t *testing.T) {
	t.Setenv("REVERSE_TRUSS", "off")
	for _, m := range witnessModes {
		t.Run(m.String(), func(t *testing.T) {
			tr := m.tracer(t, 8)
			_, root := tr.Start(context.Background(), "root", trace.WithSpanKind(trace.SpanKindServer))
			parent := startChild(tr, root, 2) // depth 1, witness (root, 1)
			own := m.witness(t, m.truss(t, parent, false))
			id := root.SpanContext().SpanID()

			// What the parent really passes on is accepted, and so is any one witness at depth 1.
			tr.Start(childOf(parent, m.withWitness(t, parent, own), 1), "ok")
			tr.Start(childOf(parent, m.withWitness(t, parent, newFanoutWitness(trace.SpanID{9}, 1)), 1), "ok")

			for name, witness := range map[string][]byte{
				"two witnesses":                     append(append([]byte(nil), own...), own...),
				"a witness without its depth":       own[:8],
				"a witness deeper than its carrier": newFanoutWitness(id, 2),
				"a witness at the root's depth":     newFanoutWitness(id, 0),
			} {
				br := m.withWitness(t, parent, witness)
				mustBreakInvariant(t, fmt.Sprintf("%v: %s", m, name), func() { tr.Start(childOf(parent, br, 1), "bad") })
			}
		})
	}
}
