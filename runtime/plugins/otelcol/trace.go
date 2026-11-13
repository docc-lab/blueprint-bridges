// Package otelcol is the run-time side of tracing: a [backend.Tracer] that exports to an OpenTelemetry
// collector, and the span processors (vanilla and the bridges) that decide what is exported and how.
package otelcol

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// SampleRatioEnv sets a head-based sampling ratio in [0,1]. The decision is made once, at the trace
// root, and inherited downstream (ParentBased(TraceIDRatioBased)), so a trace is recorded completely
// or not at all. Unset or >= 1 samples everything; <= 0 samples nothing.
const SampleRatioEnv = "OTEL_SAMPLE_RATIO"

// headSampler builds the SDK sampler from SampleRatioEnv.
func headSampler() tracesdk.Sampler {
	v := os.Getenv(SampleRatioEnv)
	if v == "" {
		return tracesdk.AlwaysSample()
	}
	r, err := strconv.ParseFloat(v, 64)
	if err != nil || r >= 1.0 {
		return tracesdk.AlwaysSample()
	}
	if r <= 0 {
		return tracesdk.NeverSample()
	}
	slog.Info("head sampling enabled", "ratio", r)
	return tracesdk.ParentBased(tracesdk.TraceIDRatioBased(r))
}

// BridgeKindEnv selects the span processor when a service starts. The wiring specs set it for each
// service variant. Values:
//
//	pb       path bridge (also used when unset)
//	cgpb     call-graph path bridge
//	sb       structural bridge
//	eb       exact bridge
//	v        vanilla: plain OTLP export, no bridge
//	rc       random checkpoints
//	priority priority export only
//	realtime partial spans (start and end events)
const BridgeKindEnv = "BRIDGE_KIND"

// OTCollectorTracer is a [backend.Tracer] that exports spans to an OpenTelemetry collector.
type OTCollectorTracer struct {
	tp trace.TracerProvider
	// startAttrBaggage reports whether the span processor adds "__bag." attributes in OnStart.
	startAttrBaggage bool
	kind             string // the processor's kind: v, pb, cgpb, sb, eb, rc, priority or realtime
}

// NewOTCollectorTracer creates a tracer that exports to the collector at addr. additionalPort is the
// collector's config discovery port, from which the bridges read their parameters.
//
// The span processor is chosen by [BridgeKindEnv]. An unknown value is a broken invariant: the
// instrumentation was built for a kind of its own, and a different processor would build wrong
// traces.
func NewOTCollectorTracer(ctx context.Context, addr string, additionalPort string) (*OTCollectorTracer, error) {
	kind := os.Getenv(BridgeKindEnv)
	spanProcessor, resolvedKind, err := newBridgeProcessor(ctx, kind, addr, additionalPort)
	if err != nil {
		return nil, err
	}
	slog.Info("✅ OTCollectorTracer initialized", "bridge_kind", resolvedKind, "addr", addr)

	// Processors that never add "__bag." attributes in OnStart say so; assume the others do. Ask
	// before wrapping: the reverse-checkpoint wrapper adds none itself.
	startAttrBaggage := true
	if r, ok := spanProcessor.(interface{ startAttributeBaggage() bool }); ok {
		startAttrBaggage = r.startAttributeBaggage()
	}

	// Lets the instrumentation finish a span's checkpoint state before Span.End.
	spanProcessor = wrapReverseCheckpointProcessor(spanProcessor)
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSampler(headSampler()),
		tracesdk.WithSpanProcessor(spanProcessor),
	)
	if preparer, ok := spanProcessor.(backend.CheckpointPreparer); ok {
		return &OTCollectorTracer{&checkpointTracerProvider{tp, preparer}, startAttrBaggage, resolvedKind}, nil
	}
	return &OTCollectorTracer{tp, startAttrBaggage, resolvedKind}, nil
}

// newBridgeProcessor creates the processor for a BRIDGE_KIND value. It also returns the kind it
// resolved to, for logging when the value was empty or unknown.
func newBridgeProcessor(ctx context.Context, kind, addr, port string) (tracesdk.SpanProcessor, string, error) {
	switch kind {
	case "", "pb":
		p, err := NewPathBridgeProcessor(ctx, addr, port)
		if err != nil {
			return nil, "pb", fmt.Errorf("failed to create path-bridge span processor: %w", err)
		}
		return p, "pb", nil
	case "cgpb":
		p, err := NewCallGraphBridgeProcessor(ctx, addr, port)
		if err != nil {
			return nil, "cgpb", fmt.Errorf("failed to create call-graph bridge span processor: %w", err)
		}
		return p, "cgpb", nil
	case "sb":
		p, err := NewStructuralBridgeProcessor(ctx, addr, port)
		if err != nil {
			return nil, "sb", fmt.Errorf("failed to create structural-bridge span processor: %w", err)
		}
		return p, "sb", nil
	case "eb":
		p, err := NewExactBridgeProcessor(ctx, addr, port)
		if err != nil {
			return nil, "eb", fmt.Errorf("failed to create exact-bridge span processor: %w", err)
		}
		return p, "eb", nil
	case "v", "vanilla":
		p, err := NewVanillaProcessor(ctx, addr, port)
		if err != nil {
			return nil, "v", fmt.Errorf("failed to create vanilla span processor: %w", err)
		}
		return p, "v", nil
	case "rc":
		p, err := NewRandomCheckpointProcessor(ctx, addr, port)
		if err != nil {
			return nil, "rc", fmt.Errorf("failed to create random-checkpoint span processor: %w", err)
		}
		return p, "rc", nil
	case "priority":
		p, err := NewPriorityProcessor(ctx, addr, port)
		if err != nil {
			return nil, "priority", fmt.Errorf("failed to create priority span processor: %w", err)
		}
		return p, "priority", nil
	case "realtime":
		p, err := NewRealTimeSpanProcessor(ctx, addr, port)
		if err != nil {
			return nil, "realtime", fmt.Errorf("failed to create realtime span processor: %w", err)
		}
		return p, "realtime", nil
	default:
		backend.InvariantBroken("BRIDGE_KIND is a known span processor kind", "BRIDGE_KIND=", kind)
		return nil, "", nil // not reached
	}
}

// StartAttributeBaggage implements [backend.AttributeBaggageReporter]. The sampler adds no
// attributes, so only the span processor matters.
func (t *OTCollectorTracer) StartAttributeBaggage() bool { return t.startAttrBaggage }

// BridgeKind returns the span processor's kind, so the instrumentation can check that it was built
// for the same bridge.
func (t *OTCollectorTracer) BridgeKind() string { return t.kind }

// GetTracerProvider implements [backend.Tracer].
func (t *OTCollectorTracer) GetTracerProvider(ctx context.Context) (trace.TracerProvider, error) {
	return t.tp, nil
}
