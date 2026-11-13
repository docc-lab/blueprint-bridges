package backend

import "fmt"

// Invariant checks for the tracing instrumentation: the wrapper templates, the gRPC interceptors and
// the HTTP middleware check conditions that only a bug in the tracing code itself can break, such as
// the bounds of child ordinals and counters and the structure of what one traced service sends
// another (carriers, reverse contexts, one value per header). A broken one panics rather than let
// the span processors build a wrong trace.
//
// Each check is one comparison on the hot path; InvariantBroken is only called when it fails, so its
// arguments are only evaluated and formatted then.

// InvariantError is the panic value of a broken tracing invariant.
type InvariantError struct {
	Rule   string // what must hold, e.g. "the caller's child ordinal is >= 1"
	Detail string // the values that broke it
}

func (e *InvariantError) Error() string {
	return "tracing invariant broken: " + e.Rule + " (" + e.Detail + ")"
}

// InvariantBroken panics with an *InvariantError.  details are formatted with fmt.Sprint.
//
// Kept out of line so that a check adds only a compare and a call to the code around it.
//
//go:noinline
func InvariantBroken(rule string, details ...any) {
	panic(&InvariantError{Rule: rule, Detail: fmt.Sprint(details...)})
}

// IsPackedRetCtx reports whether a reverse context is in the packed binary envelope
// (reversetruss_packed.go), the only form any traced service sends: what the backend merges and
// PrepareCheckpoint returns, before a transport turns it into text.  Empty means "none" and is not
// packed.
func IsPackedRetCtx(retCtx string) bool {
	return len(retCtx) > 0 && retCtx[0] == packedRetCtxVersion
}

// CheckBridgeKind checks that the span processor behind tracer runs the bridge an instrumentation
// was built for. instrumentation is the kind the instrumentation implements: "vanilla", "pb", "cgpb"
// or "sb". A tracer that does not report its kind (a BridgeKind method) is not checked, and neither
// are processor kinds without a matching instrumentation. The bridge kind is chosen twice, at build
// time for the instrumentation (OT_BRIDGE) and at run time for the processor (BRIDGE_KIND); with two
// different bridges, spans would be propagated by one bridge's rules and classified by another's.
func CheckBridgeKind(tracer any, instrumentation string) {
	r, ok := tracer.(interface{ BridgeKind() string })
	if !ok {
		return
	}
	kind := r.BridgeKind()
	var want string
	switch kind {
	case "v":
		want = "vanilla"
	case "pb", "cgpb", "sb":
		want = kind
	default:
		return
	}
	if want != instrumentation {
		InvariantBroken("the span processor (BRIDGE_KIND) is the bridge the instrumentation was built for (OT_BRIDGE)",
			"processor=", kind, " instrumentation=", instrumentation)
	}
}
