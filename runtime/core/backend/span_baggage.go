package backend

// Baggage that a tracing SDK keeps beside the span.
//
// The instrumentation builds each outgoing carrier's baggage from the upstream baggage plus the
// span's "__bag."-prefixed attributes, Blueprint's convention for arbitrary baggage. An SDK that keeps
// its own propagation state beside the span instead (the bridges) registers a source here. Its
// entries are merged first and the span's "__bag." attributes applied after, so other producers'
// baggage still propagates and takes precedence.

import (
	"go.opentelemetry.io/otel/trace"
)

// SpanBaggageSource adds the baggage entries the SDK keeps for a span to baggage (any number of
// entries; none if it knows nothing about the span).
type SpanBaggageSource func(sc trace.SpanContext, baggage map[string]string)

// Set once, from the tracing SDK's package init, before any request runs. (A plain variable:
// the Blueprint compiler parses this package and its Go parser does not accept generics.)
var spanBaggageSource SpanBaggageSource

// RegisterSpanBaggageSource installs the process's source (the tracing SDK, at init).
func RegisterSpanBaggageSource(f SpanBaggageSource) { spanBaggageSource = f }

// AppendSpanBaggage adds the registered source's entries for span to baggage.
func AppendSpanBaggage(span trace.Span, baggage map[string]string) {
	if f := spanBaggageSource; f != nil {
		f(span.SpanContext(), baggage)
	}
}

// AttributeBaggageReporter is implemented by a tracer (a [Tracer]) that knows whether anything in
// its SDK adds "__bag." attributes to a span while the span starts (span processors' OnStart, the
// sampler). The gRPC interceptors turn a new span's "__bag." attributes into baggage right after
// starting it, before application code can touch it, so only the SDK can have written them then;
// when the tracer reports false they skip reading the span's attributes. A tracer that does not
// implement this is assumed to write them.
type AttributeBaggageReporter interface {
	StartAttributeBaggage() bool
}
