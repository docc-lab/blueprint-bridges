package otelcol

import "github.com/blueprint-uservices/blueprint/runtime/core/backend"

// Tomislav-RetCtx (user 2026-11-06: "add bounds checks and assertions where appropriate inside of the
// vanilla, pb, cgpb, and sb sdk-side processors"): the failure paths of the processors' invariant
// checks.  Each check is a comparison on the span path; when one fails these format the values and
// call backend.InvariantBroken (runtime/core/backend/invariant.go), which panics.  They are out of
// line and take plain ints and strings so that a check adds a compare and a short call to OnStart /
// OnEnd, whose stack frames earlier profiling found worth keeping small (utils/grpc_interceptors/
// README.md, "Stack growth").
//
// Checked are only conditions that a bug in this code breaks, never the input of other systems:
// what a parent span of the same bridge wrote (`_br`), what the instrumentation of this process
// wrote (seqNum, remEndEvents), the store-once / take-once wire table, ordinal and distance bounds,
// and the export bytes' own framing.

//go:noinline
func brokenInt(rule, what string, n, bound int) {
	backend.InvariantBroken(rule, what, "=", n, " bound=", bound)
}

//go:noinline
func brokenRule(rule string) {
	backend.InvariantBroken(rule)
}
