package otelcol

import (
	"context"
	"math"
	"math/rand/v2"
	"os"
	"strconv"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const reverseDepthBaggageKey = "__rt_depth"

// reverseDepthAttribute is no longer written; the export path still strips it, and a test checks it
// never reaches a span.
const reverseDepthAttribute = "__bag." + reverseDepthBaggageKey

// checkpointRejectedAttribute is read by the export classifier at OnEnd, after the per-span state
// is gone.
const checkpointRejectedAttribute = "__bag.rev_rejected"

// reverseSpanState is what the response path needs to know about a span between OnStart and
// PrepareCheckpoint. Keeping it in a table rather than in span attributes avoids a locked dedupe of
// the attribute set on every read, and keeps the exported attribute set short. Entries are removed
// at OnEnd.
type reverseSpanState struct {
	depth     uint64
	scheduled bool
	prepared  bool
	carried   string
	// What PrepareCheckpoint decided for a span with wire state, read at OnEnd by classification and
	// export. Spans without wire state (vanilla, fixed-distance mode) use attributes instead.
	rc       string // trusses this span took over (packed envelope), exported inside `_br`
	rejected bool   // a leaf that declined to be a checkpoint
}

// reverseCheckpointProcessor wraps a span processor with the response path: spans can absorb the
// trusses their children return (becoming checkpoints), pass them on upstream, or, as unscheduled
// leaves, decline to be checkpoints. Every span still reaches the wrapped processor; the response
// path only changes a span's checkpoint role.
type reverseCheckpointProcessor struct {
	sdktrace.SpanProcessor
	states         *reverseStateTable
	kind           string
	scheduled      func([]attribute.KeyValue) bool
	reverseRange   checkpointRange
	reversePolicy  reversePolicy
	leafRejectRate float64
	root           bool
}

func wrapReverseCheckpointProcessor(next sdktrace.SpanProcessor) sdktrace.SpanProcessor {
	if !backend.ReverseTrussEnabled() {
		return next
	}
	p := &reverseCheckpointProcessor{
		SpanProcessor: next, root: backend.IsRoot(), states: newReverseStateTable(),
	}
	var distance int64 = 1
	switch next := next.(type) {
	case *PathBridgeProcessor:
		p.kind, p.scheduled = backend.SegPathCheckpoint, isScheduledPathCheckpoint
		p.reverseRange, distance = next.checkpointRange, next.checkpointDistance
		p.reversePolicy = next.reversePolicy
	case *CallGraphBridgeProcessor:
		p.kind, p.scheduled = backend.SegCallGraphCheckpoint, isScheduledPathCheckpoint
		p.reverseRange, distance = next.checkpointRange, next.checkpointDistance
		p.reversePolicy = next.reversePolicy
	case *StructuralBridgeProcessor:
		p.kind, p.scheduled = backend.SegStructuralCheckpoint, isScheduledPathCheckpoint
		p.reverseRange, distance = next.checkpointRange, next.checkpointDistance
		p.reversePolicy = next.reversePolicy
	case *VanillaProcessor:
		p.kind = backend.SegVanillaCheckpoint
		p.scheduled = func([]attribute.KeyValue) bool { return true }
	default:
		return next
	}
	if !p.reverseRange.enabled() {
		// Fixed-distance mode: the response path uses the same fixed distance.
		n := int(min(max(distance, 1), 256))
		p.reverseRange = checkpointRange{n, n}
	}
	rate, err := strconv.ParseFloat(os.Getenv("RT_LEAF_REJECT"), 64)
	if err == nil && !math.IsNaN(rate) && rate > 0 {
		p.leafRejectRate = math.Min(rate, 1)
	}
	return p
}

func (p *reverseCheckpointProcessor) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	p.SpanProcessor.OnStart(parent, span)
	// Range-mode PB/CGPB and SB have depth and the scheduling decision in the wire entry.
	if w := bridgeWires.load(span.SpanContext().SpanID()); w != nil {
		w.rev = reverseSpanState{depth: uint64(w.depth), scheduled: w.priority}
		return
	}
	depth := uint64(0)
	if value := backend.GetBaggageFromContext(parent)[reverseDepthBaggageKey]; value != "" {
		if n, err := strconv.ParseUint(value, 10, 64); err == nil && n < math.MaxUint64 {
			depth = n + 1
		}
	}
	// One Attributes call serves both the depth and the scheduling decision: each call locks the
	// span and dedupes its attributes through a new map.
	attrs := span.Attributes()
	if p.kind == backend.SegPathCheckpoint || p.kind == backend.SegCallGraphCheckpoint || p.kind == backend.SegStructuralCheckpoint {
		// These formats carry the absolute depth, which is used even if the parent sent no response-path
		// depth.
		for _, attr := range attrs {
			if attr.Key == AttrBREmit {
				if d, ok := decodeBRDepth(attr.Value.AsString()); ok {
					depth = uint64(d)
				}
			}
		}
	}
	// Vanilla has no truss depth; the response-path baggage carries it. The scheduling decision is
	// recorded now, before any returned truss can make the span a checkpoint at OnEnd.
	p.states.store(span.SpanContext().SpanID(), &reverseSpanState{
		depth: depth, scheduled: p.scheduled(attrs),
	})
}

// OnEnd forwards the span, then drops its state. Spans with a wire entry keep the state there, and the
// bridge's OnEnd frees it.
func (p *reverseCheckpointProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	p.SpanProcessor.OnEnd(s)
	p.states.delete(s.SpanContext().SpanID()) // vanilla and fixed-distance spans
}

func (p *reverseCheckpointProcessor) state(id trace.SpanID) *reverseSpanState {
	if w := bridgeWires.load(id); w != nil {
		return &w.rev
	}
	if state := p.states.load(id); state != nil {
		return state
	}
	// A span not seen at OnStart: treat it as depth 0 and unscheduled rather than failing the request.
	return &reverseSpanState{}
}

// PrepareCheckpoint makes the span's response-path decision before Span.End freezes its attributes:
// returned is the reverse context its children sent back. Each truss spends one hop (or one
// probability trial) per span, client or server; preparing a span again spends nothing. It returns
// the reverse context to pass on upstream.
func (p *reverseCheckpointProcessor) PrepareCheckpoint(span trace.Span, returned string) string {
	return p.prepare(span, returned, -1)
}

// PrepareServerCheckpoint implements backend.ServerCheckpointPreparer: the caller says how many
// children the server span had, so its attributes need not be read back for that.
func (p *reverseCheckpointProcessor) PrepareServerCheckpoint(span trace.Span, returned string, children int) string {
	return p.prepare(span, returned, children)
}

// prepare is PrepareCheckpoint; children is the span's child count, or -1 if the caller does not
// know it (then a server span's children attributes are read).
func (p *reverseCheckpointProcessor) prepare(span trace.Span, returned string, children int) string {
	if children < -1 {
		brokenInt("a server span's child count is >= 0 (or -1: unknown)", "children", children, -1)
	}
	s, ok := span.(sdktrace.ReadWriteSpan)
	if !ok || !s.IsRecording() {
		return ""
	}
	state := p.state(s.SpanContext().SpanID())
	if state.prepared {
		// Already prepared: return what the first call decided to pass on.
		return state.carried
	}
	state.prepared = true
	depth, scheduled := state.depth, state.scheduled
	// With wire state, the truss and (from PrepareServerCheckpoint) the child count are known. The
	// span's attributes are read only for what is not: a fixed-distance bridge's truss, SB's force_lp,
	// or a server span's children when the caller did not say.
	var truss []byte
	var forceLP bool
	var attrs []attribute.KeyValue
	server := s.SpanKind() == trace.SpanKindServer
	w := bridgeWires.load(s.SpanContext().SpanID())
	if w != nil {
		truss = w.emit
	}
	if w == nil || p.kind == backend.SegStructuralCheckpoint || (server && children < 0) {
		attrs = s.Attributes()
		for _, attr := range attrs {
			switch attr.Key {
			case AttrBREmit:
				if w == nil {
					truss, _ = decodeBR(attr.Value.AsString())
				}
			case AttrForceLP:
				forceLP = p.kind == backend.SegStructuralCheckpoint && attr.Value.AsBool()
			}
		}
	}
	hasChildren := children > 0
	if children < 0 {
		hasChildren = spanHasChildrenIn(attrs)
	}
	root := depth == 0 || (p.root && server)
	if returned != "" {
		backend.CountTrussReceived()
		var emitted string
		if !forceLP {
			emitted, returned = p.reversePolicy.route(returned, p.reversePolicy.terminates(scheduled, root), depth, rand.Float64)
		}
		if emitted != "" {
			// A span with wire state keeps the reverse context there, raw; the export folds it into
			// `_br`. A span without wire state gets the `_rc` attribute, as text.
			if w != nil {
				w.rev.rc = emitted
			} else {
				s.SetAttributes(attribute.String(backend.ReverseTrussCheckpointKey, backend.RetCtxText(emitted)))
			}
			backend.CountCheckpoint()
			backend.SampleLogCheckpoint(emitted)
		}
	}
	// Only forced, unscheduled leaves may decline. Scheduled checkpoints keep their own data and
	// absorb every returned truss, unless reverse_passthrough is on.
	if server && !hasChildren && !forceLP && !scheduled && !root && p.leafRejectRate > 0 && len(truss) > 0 {
		if rand.Float64() < p.leafRejectRate {
			var own string
			if p.reversePolicy.probabilistic() {
				// Receivers use the leaf's depth; probability mode draws no TTL.
				own = backend.EncodeCheckpointRetCtx(s.SpanContext().SpanID(), depth, p.kind, truss)
			} else {
				own = backend.EncodeCheckpointRetCtxWithTTL(s.SpanContext().SpanID(), depth, p.kind, truss, p.reverseRange.newTTL())
			}
			returned = backend.MergeRetCtx(returned, own)
			if w != nil {
				w.rev.rejected = true
			} else {
				s.SetAttributes(attribute.Bool(checkpointRejectedAttribute, true))
			}
			backend.CountLeafReject()
		} else {
			backend.CountLocalCheckpoint()
		}
	}
	state.carried = returned
	return returned
}

// checkpointTracerProvider exposes the preparation hook without replacing OTel
// spans or their End method (including its panic and sampling behavior).
type checkpointTracerProvider struct {
	trace.TracerProvider
	backend.CheckpointPreparer
}

// PrepareServerCheckpoint implements backend.ServerCheckpointPreparer when the preparer does.
func (c *checkpointTracerProvider) PrepareServerCheckpoint(span trace.Span, returned string, children int) string {
	if p, ok := c.CheckpointPreparer.(backend.ServerCheckpointPreparer); ok {
		return p.PrepareServerCheckpoint(span, returned, children)
	}
	return c.CheckpointPreparer.PrepareCheckpoint(span, returned)
}

// isPathCheckpoint is PB/CGPB classification, for callers without the attribute slice.
func isPathCheckpoint(s sdktrace.ReadOnlySpan) bool {
	return pathCheckpoint(s.SpanKind(), s.Attributes(), bridgeWires.load(s.SpanContext().SpanID()))
}

// pathCheckpoint classifies a PB/CGPB span from the attribute slice the caller holds. The
// scheduling decision comes from the wire entry in range mode, else from __bag.prio.
func pathCheckpoint(kind trace.SpanKind, attrs []attribute.KeyValue, w *bridgeWire) bool {
	var priority, hasPriority, rejected, reverseCheckpoint bool
	if w != nil {
		priority, hasPriority = w.priority, true
		rejected, reverseCheckpoint = w.rev.rejected, w.rev.rc != ""
	}
	for _, attr := range attrs {
		switch attr.Key {
		case checkpointRejectedAttribute:
			rejected = rejected || attr.Value.AsBool()
		case backend.ReverseTrussCheckpointKey:
			reverseCheckpoint = reverseCheckpoint || attr.Value.AsString() != ""
		case AttrBagPrio:
			priority, hasPriority = attr.Value.AsInt64() == 1, true
		}
	}
	return reverseCheckpoint || (!rejected && hasPriority && (priority || (kind == trace.SpanKindServer && !spanHasChildrenIn(attrs))))
}

func isScheduledPathCheckpoint(attrs []attribute.KeyValue) bool {
	for _, attr := range attrs {
		if attr.Key == AttrBagPrio {
			return attr.Value.AsInt64() == 1
		}
	}
	return false
}

// The In variants take the attribute slice the caller already holds.
func spanHasChildren(s sdktrace.ReadOnlySpan) bool { return spanHasChildrenIn(s.Attributes()) }

func spanHasChildrenIn(attrs []attribute.KeyValue) bool {
	for _, attr := range attrs {
		switch attr.Key {
		case "hasChildren":
			if attr.Value.AsBool() {
				return true
			}
		case "childCount", "eventCount":
			if attr.Value.AsInt64() > 0 {
				return true
			}
		}
	}
	return false
}

// structuralCheckpoint is SB's classification from the attribute slice the caller holds:
// the PB/CGPB window classification, with force_lp keeping synthetic pressure spans ordinary.
func structuralCheckpoint(kind trace.SpanKind, attrs []attribute.KeyValue, w *bridgeWire) bool {
	for _, attr := range attrs {
		if attr.Key == AttrForceLP && attr.Value.AsBool() {
			return false
		}
	}
	return pathCheckpoint(kind, attrs, w)
}

func (p *StructuralBridgeProcessor) isCheckpoint(s sdktrace.ReadOnlySpan) bool {
	var forceLP, rejected, reverseCheckpoint bool
	for _, attr := range s.Attributes() {
		switch attr.Key {
		case checkpointRejectedAttribute:
			rejected = attr.Value.AsBool()
		case backend.ReverseTrussCheckpointKey:
			reverseCheckpoint = attr.Value.AsString() != ""
		case AttrForceLP:
			forceLP = attr.Value.AsBool()
		}
	}
	// SB shares the PB/CGPB classification; force_lp keeps synthetic spans low priority.
	_, _, _ = rejected, reverseCheckpoint, forceLP
	return !forceLP && isPathCheckpoint(s)
}
