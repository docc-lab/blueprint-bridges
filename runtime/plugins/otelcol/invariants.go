package otelcol

import (
	"encoding/binary"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
)

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

//go:noinline
func brokenOrder(rule, what string, n, bound int64) {
	backend.InvariantBroken(rule, what, "=", n, " bound=", bound)
}

// checkParentWitness checks the fan-out witness in a parent's truss, when it has one: exactly one
// newFanoutWitness, recorded between the root and the parent.
//
//go:noinline
func checkParentWitness(w []byte, parentDepth int) {
	if len(w) <= 8 {
		brokenInt("a fan-out witness is a span ID and a depth", "witness bytes", len(w), 9)
	}
	depth, n := binary.Uvarint(w[8:])
	if n <= 0 || 8+n != len(w) {
		brokenInt("a truss carries at most one fan-out witness", "witness bytes", len(w), 8+max(n, 1))
	}
	if depth < 1 || depth > uint64(parentDepth) {
		brokenInt("a fan-out witness lies between the root and the span that carries it", "witness depth",
			int(min(depth, uint64(parentDepth)+1)), parentDepth)
	}
}

// checkExportCounts checks how a processor's span counters relate: a span is received, then
// flushed, then sent or dropped, and each counter is incremented after the one before it. The
// arguments must be loaded in the reverse order (sent and dropped first, received last), so an
// increment that races the loads can only make the later counter larger.
func checkExportCounts(sent, dropped, flushed, received int64) {
	if sent+dropped > flushed {
		brokenOrder("every sent or dropped span was flushed", "sent+dropped", sent+dropped, flushed)
	}
	if flushed > received {
		brokenOrder("every flushed span was received", "flushed", flushed, received)
	}
}

// checkPriorityCounts checks that the checkpoint and low-priority counts of something add up to no
// more than its total; cp and lp must be loaded before total (see checkExportCounts).
func checkPriorityCounts(what string, cp, lp, total int64) {
	if cp+lp > total {
		brokenOrder("checkpoint and low-priority counts add up to the total", what, cp+lp, total)
	}
}

// checkDrainedCounts checks the exact relations once every export has finished (Shutdown).
func checkDrainedCounts(sent, dropped, flushed int64) {
	if sent+dropped != flushed {
		brokenOrder("after shutdown every flushed span was sent or dropped", "sent+dropped", sent+dropped, flushed)
	}
}
