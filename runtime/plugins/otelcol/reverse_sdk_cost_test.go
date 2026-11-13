package otelcol

// Benchmarks the response path's per-span SDK cost: the sequence of attribute reads and writes the
// client instrumentation performs around a span, against a real recording span, with and without
// the response-path wrapper. ReadWriteSpan.Attributes() is not a cheap read: each call locks the
// span and dedupes its attributes. The wire-format counterpart is
// runtime/core/backend/retctx_cost_test.go.
//
// Run: RETCTX_COST=1 go test ./runtime/plugins/otelcol -run XXX \
//        -bench BenchmarkReverseSpan -benchmem -benchtime 200000x

import (
	"context"
	"os"
	"testing"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func skipUnlessSDKCost(tb testing.TB) {
	if os.Getenv("RETCTX_COST") == "" {
		tb.Skip("set RETCTX_COST=1")
	}
}

// reverseSpanHarness builds a provider whose PB processor is optionally wrapped
// by the reverse checkpoint processor, exactly as wrapReverseCheckpointProcessor
// decides at startup from REVERSE_TRUSS.
func reverseSpanHarness(reverse bool) (trace.TracerProvider, context.Context) {
	var processor sdktrace.SpanProcessor = &PathBridgeProcessor{checkpointDistance: 3}
	if reverse {
		processor = wrapReverseCheckpointProcessor(processor)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(processor))
	var provider trace.TracerProvider = tp
	if preparer, ok := processor.(backend.CheckpointPreparer); ok {
		provider = &checkpointTracerProvider{tp, preparer}
	}
	return provider, checkpointParentContext("pb", 3)
}

// oneClientSpan replays what the generated client wrapper does per span.
func oneClientSpan(tp trace.TracerProvider, parent context.Context, reverse bool, retCtx string) {
	ctx, span := tp.Tracer("bench").Start(parent, "ClientCall", trace.WithSpanKind(trace.SpanKindClient))
	_ = ctx
	if reverse && retCtx != "" {
		// plugins/opentelemetry/ir_ot_client.go: hand the response to the live span,
		// let the SDK route it, then read back whatever was not consumed.
		span.SetAttributes(attribute.String(backend.ReverseTrussInputKey, retCtx))
		// The wrapper now takes the carrier from the return value instead of
		// re-scanning the span for it.
		if out, prepared := backend.PrepareCheckpoint(tp, span, retCtx); prepared {
			_ = out
		}
	}
	span.End()
}

func benchReverseSpan(b *testing.B, reverse bool) {
	skipUnlessSDKCost(b)
	tp, parent := reverseSpanHarness(reverse)
	// A returned truss of the size the cluster actually carries: one packed
	// checkpoint segment.
	retCtx := backend.EncodeCheckpointRetCtxWithTTL(trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}, 3,
		backend.SegPathCheckpoint, make([]byte, 19), 2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oneClientSpan(tp, parent, reverse, retCtx)
	}
}

func BenchmarkReverseSpanOff(b *testing.B) { benchReverseSpan(b, false) }
func BenchmarkReverseSpanOn(b *testing.B)  { benchReverseSpan(b, true) }

// BenchmarkReverseSpanAttributesOnly isolates the attribute-access cost alone:
// how much does one extra Attributes() scan cost on a span of realistic width?
func BenchmarkReverseSpanAttributesOnly(b *testing.B) {
	skipUnlessSDKCost(b)
	tp, parent := reverseSpanHarness(true)
	_, span := tp.Tracer("bench").Start(parent, "ClientCall", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	readable, ok := span.(interface {
		Attributes() []attribute.KeyValue
	})
	if !ok {
		b.Skip("span is not readable")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = readable.Attributes()
	}
}

// BenchmarkReverseSpanParallel runs the same sequence from many goroutines at once, as a service
// that fans out does, to expose contention on the per-span state.
//
//	Run: RETCTX_COST=1 go test ./runtime/plugins/otelcol -run XXX \
//	       -bench BenchmarkReverseSpanParallel -benchmem -cpu 1,8,40
func BenchmarkReverseSpanParallel(b *testing.B) {
	skipUnlessSDKCost(b)
	tp, parent := reverseSpanHarness(true)
	retCtx := backend.EncodeCheckpointRetCtxWithTTL(trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}, 3,
		backend.SegPathCheckpoint, make([]byte, 19), 2)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			oneClientSpan(tp, parent, true, retCtx)
		}
	})
}

func BenchmarkReverseSpanParallelOff(b *testing.B) {
	skipUnlessSDKCost(b)
	tp, parent := reverseSpanHarness(false)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			oneClientSpan(tp, parent, false, "")
		}
	})
}
