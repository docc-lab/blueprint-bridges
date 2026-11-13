package otelcol

import "github.com/blueprint-uservices/blueprint/runtime/core/backend"

// Failure paths of the processors' invariant checks. A check is an inline comparison; when it
// fails, one of these reports the rule and values through backend.InvariantBroken, which panics.
// They are kept out of line, with plain arguments, so a check costs OnStart and OnEnd only a
// compare and a short call and does not grow their stack frames.
//
// The checks cover only what a bug in this package could break, never input from other systems:
// `_br` written by a parent span of the same bridge, values this process's instrumentation wrote
// (seqNum, remEndEvents), the wire table's store-once / take-once rule, ordinal and distance
// bounds, and the framing of the exported bytes.

//go:noinline
func brokenInt(rule, what string, n, bound int) {
	backend.InvariantBroken(rule, what, "=", n, " bound=", bound)
}

//go:noinline
func brokenRule(rule string) {
	backend.InvariantBroken(rule)
}
