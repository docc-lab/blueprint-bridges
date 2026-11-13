package backend

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ReverseBaggageKey is an SDK-to-instrumentation carrier. Its value becomes
// retCtx on the RPC response; it must not enter forward request baggage.
const ReverseBaggageKey = "__bag.rev"

// ReverseTrussInputKey passes received trusses to the SDK on a live span. The SDK alone decides
// whether to absorb them or pass them on in __bag.rev.
const ReverseTrussInputKey = "__bag.rev_in"

// IsReverseBaggageKey identifies private return carriers and SDK decision state.
// None of these attributes should enter forward baggage or span exports.
func IsReverseBaggageKey(key attribute.Key) bool {
	return key == ReverseBaggageKey || strings.HasPrefix(string(key), ReverseBaggageKey+"_")
}

// CheckpointPreparer is implemented by Blueprint's tracing SDK. Its synchronous
// hook finalizes checkpointing while the span is still mutable: OTel OnEnd gets
// a read-only snapshot, which is too late to write reverse baggage.
type CheckpointPreparer interface {
	// It returns the reverse context to pass upstream, so the caller need not read it back off
	// the span (which locks the span and dedupes its attributes).
	PrepareCheckpoint(span trace.Span, returned string) string
}

// PrepareCheckpoint asks the SDK to finish its checkpoint decision. Call it once, after attaching the
// received trusses and the final span attributes, and before reading reverse baggage and ending the
// span. False means the SDK could not decide; the instrumentation must then pass the received
// trusses on unchanged.
func PrepareCheckpoint(provider trace.TracerProvider, span trace.Span, returned string) (string, bool) {
	if !span.IsRecording() {
		return "", false
	}
	if preparer, ok := provider.(CheckpointPreparer); ok {
		return preparer.PrepareCheckpoint(span, returned), true
	}
	return "", false
}

// ServerCheckpointPreparer is a CheckpointPreparer that takes a server span's child count from the
// caller; otherwise the SDK reads it back from the span's attributes, which locks the span and copies
// them through a map.
type ServerCheckpointPreparer interface {
	PrepareServerCheckpoint(span trace.Span, returned string, children int) string
}

// PrepareServerCheckpoint is PrepareCheckpoint for a server span whose child count (the calls its
// handler made) the caller knows.  Without a ServerCheckpointPreparer it is PrepareCheckpoint.
func PrepareServerCheckpoint(provider trace.TracerProvider, span trace.Span, returned string, children int) (string, bool) {
	if !span.IsRecording() {
		return "", false
	}
	if preparer, ok := provider.(ServerCheckpointPreparer); ok {
		return preparer.PrepareServerCheckpoint(span, returned, children), true
	}
	return PrepareCheckpoint(provider, span, returned)
}

// ReadReverseBaggage extracts the SDK's encoded return carrier from a recording
// span. Instrumentation carries it upstream or adds it to the fan-in accumulator.
func ReadReverseBaggage(span trace.Span) string {
	if readable, ok := span.(interface{ Attributes() []attribute.KeyValue }); ok {
		for _, attr := range readable.Attributes() {
			if attr.Key == ReverseBaggageKey {
				return attr.Value.AsString()
			}
		}
	}
	return ""
}
