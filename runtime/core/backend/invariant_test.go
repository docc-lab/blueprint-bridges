package backend

import (
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// InvariantBroken panics with an *InvariantError naming the rule and the values.
func TestInvariantBroken(t *testing.T) {
	defer func() {
		r := recover()
		err, ok := r.(error)
		var ie *InvariantError
		if !ok || !errors.As(err, &ie) {
			t.Fatalf("panic value %#v is not an *InvariantError", r)
		}
		if ie.Rule != "ordinal >= 1" || ie.Detail != "seqNum=0" {
			t.Errorf("got rule %q detail %q", ie.Rule, ie.Detail)
		}
		if !strings.Contains(err.Error(), "ordinal >= 1") || !strings.Contains(err.Error(), "seqNum=0") {
			t.Errorf("message %q lacks the rule or the values", err.Error())
		}
	}()
	InvariantBroken("ordinal >= 1", "seqNum=", 0)
	t.Fatal("InvariantBroken returned")
}

// IsPackedRetCtx accepts exactly what a packed envelope starts with.
func TestIsPackedRetCtx(t *testing.T) {
	merged := MergeRetCtx(string([]byte{packedRetCtxVersion}), "")
	for _, c := range []struct {
		in   string
		want bool
	}{
		{"", false},
		{string([]byte{packedRetCtxVersion}), true},
		{merged, merged != ""},
		{"eyJ0cnVzcyI6W119", false},                                 // the legacy JSON envelope's text
		{RetCtxText(string([]byte{packedRetCtxVersion, 0})), false}, // text form: transports only
	} {
		if got := IsPackedRetCtx(c.in); got != c.want {
			t.Errorf("IsPackedRetCtx(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// mustBreakInvariant fails the test unless f panics with an *InvariantError.
func mustBreakInvariant(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		err, ok := r.(error)
		var ie *InvariantError
		if !ok || !errors.As(err, &ie) {
			t.Errorf("%s: want an *InvariantError panic, got %#v", name, r)
		}
	}()
	f()
}

// The response path's checks: a checkpoint truss names a valid span and has a checkpoint kind, a
// packed segment always carries both, and a reverse context being merged decodes.
func TestReverseContextInvariants(t *testing.T) {
	valid := EncodeCheckpointRetCtx(trace.SpanID{1}, 3, SegPathCheckpoint, []byte{1, 2})
	mustBreakInvariant(t, "invalid span ID", func() { EncodeCheckpointRetCtx(trace.SpanID{}, 3, SegPathCheckpoint, nil) })
	mustBreakInvariant(t, "ancestry kind as a checkpoint", func() { EncodeCheckpointRetCtx(trace.SpanID{1}, 3, SegAMQ, nil) })
	mustBreakInvariant(t, "unknown kind packed", func() { appendPackedSegment(nil, TrussSegment{Kind: "x", Data: make([]byte, 9)}) })
	mustBreakInvariant(t, "segment without a location packed", func() {
		appendPackedSegment(nil, TrussSegment{Kind: SegPathCheckpoint, Data: make([]byte, 8)})
	})
	mustBreakInvariant(t, "garbage merged before", func() { MergeRetCtx("not a reverse context", valid) })
	mustBreakInvariant(t, "garbage merged after", func() { MergeRetCtx(valid, "not a reverse context") })
	// What the processors really produce passes, including every kind and the TTL forms.
	for _, kind := range []string{SegPathCheckpoint, SegCallGraphCheckpoint, SegStructuralCheckpoint, SegVanillaCheckpoint} {
		merged := MergeRetCtx(valid, EncodeCheckpointRetCtxWithTTL(trace.SpanID{2}, 4, kind, nil, 0))
		if cps, err := DecodeReturnedCheckpoints(merged); err != nil || len(cps) != 2 {
			t.Fatalf("%s: %v %+v", kind, err, cps)
		}
	}
}
