package otelcol

// Direct OTLP encoding for the export path.
//
// The processors do not build protobuf objects for spans or use the OTLP exporter client.
// encodeSpan writes a span's OTLP bytes straight from the SDK's snapshot (one allocation per span),
// buildBatch concatenates them into an ExportTraceServiceRequest, and otlpConn sends the bytes over
// a plain gRPC connection with a codec that passes them through. The collector receives standard
// OTLP/gRPC. (The older processors eb, rc, priority and realtime still use the exporter client.)
//
// The bytes are what proto.Marshal produced for the equivalent objects, field for field
// (otlp_wire_test.go checks this). Two consequences worth knowing: every span has a Status message,
// even an empty one, and the status code is the SDK's codes.Code value unmapped (the SDK and OTLP
// number Ok and Error the other way round).
//
// Wire format used here: a field is a tag byte (field number << 3 | wire type) and a value; wire
// type 0 is a varint, 1 a little-endian 64-bit value, 2 a length-prefixed byte string (also used
// for strings and nested messages). proto3 omits fields at their default value, except oneof
// members (AnyValue) and set message fields, which are written even when empty.

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// --- The connection ---

// otlpExportMethod is the OTLP/gRPC trace export method (opentelemetry.proto.collector.trace.v1).
const otlpExportMethod = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"

// otlpConn is a plain gRPC connection to the collector's OTLP endpoint, used with pre-encoded
// requests.
type otlpConn struct {
	endpoint string
	cc       *grpc.ClientConn
}

// dialOTLP opens an insecure connection; grpc.NewClient connects lazily, on the first call.
func dialOTLP(endpoint string) (*otlpConn, error) {
	cc, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUserAgent("blueprint-otelcol"))
	if err != nil {
		return nil, err
	}
	return &otlpConn{endpoint: endpoint, cc: cc}, nil
}

// export sends one encoded ExportTraceServiceRequest. ctx carries the deadline and the outgoing
// metadata (bridges-priority). The error is the gRPC status.
func (c *otlpConn) export(ctx context.Context, req []byte) error {
	var resp []byte
	if err := c.cc.Invoke(ctx, otlpExportMethod, req, &resp, grpc.ForceCodecV2(rawCodec{})); err != nil {
		return err
	}
	// A partial success counts as success; the collector's priority processor refuses whole
	// batches.
	if len(resp) > 0 {
		var r coltracepb.ExportTraceServiceResponse
		if proto.Unmarshal(resp, &r) == nil && r.PartialSuccess != nil {
			if n, msg := r.PartialSuccess.GetRejectedSpans(), r.PartialSuccess.GetErrorMessage(); n != 0 || msg != "" {
				slog.Warn("OTLP partial success", "rejected_spans", n, "error_message", msg)
			}
		}
	}
	return nil
}

func (c *otlpConn) close() error { return c.cc.Close() }

// rawCodec passes already-encoded request bytes to gRPC and returns the response bytes as they are.
// It is named "proto" so the content type stays application/grpc+proto.
//
// Marshal copies the request into a buffer from gRPC's pool, which gRPC frees once the bytes are
// written. The copy lets the batch return its own buffer to batchBufs as soon as the export is
// over, with nothing in gRPC still referencing it.
type rawCodec struct{}

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: want []byte, got %T", v)
	}
	return mem.BufferSlice{mem.Copy(b, mem.DefaultBufferPool())}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	out, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: want *[]byte, got %T", v)
	}
	*out = data.Materialize()
	return nil
}

func (rawCodec) Name() string { return "proto" }

// --- The buffer entry and the batch ---

// wireSpan is an ended span as the processors buffer it: its encoded Span message and what the flush
// needs to group and account for it. scope points to the interned scope, which keeps the entry
// small for the flush-time sort.
type wireSpan struct {
	body    []byte        // the Span message (without the ScopeSpans.spans tag and length)
	scope   *wireScope    // groups spans into ScopeSpans at flush time (sortByScope)
	traceID trace.TraceID // for the refused-trace census of a dropped batch
}

// wireBatch is one export: the request bytes and the spans in it. spans is a sub-slice of the flush's
// snapshot, and the request buffer comes from batchBufs.
type wireBatch struct {
	req   []byte
	n     int64
	spans []wireSpan
	buf   *[]byte // the buffer from batchBufs that req was written into
}

// batchBufs is a free list of request buffers, reused from flush to flush. A batch keeps its buffer
// until release, after it was sent or dropped (and retried, if it was).
//
// It is a free list rather than a sync.Pool because the garbage collector empties a sync.Pool at
// every cycle, and a loaded service collects many times between two flushes. It holds at most
// batchBufsKept buffers of at most batchBufMax bytes; release drops buffers that grew larger.
var batchBufs = make(chan *[]byte, batchBufsKept)

const (
	batchBufsKept = 32        // more than one flush uses
	batchBufInit  = 32 << 10  // about one 100 ms batch of a loaded service
	batchBufMax   = 256 << 10 // larger buffers are not kept
)

func getBatchBuf() *[]byte {
	select {
	case b := <-batchBufs:
		return b
	default:
		b := make([]byte, 0, batchBufInit)
		return &b
	}
}

func putBatchBuf(b *[]byte) {
	if cap(*b) > batchBufMax {
		return
	}
	*b = (*b)[:0]
	select {
	case batchBufs <- b:
	default: // list full: let the GC have it
	}
}

// release returns the batch's buffer to the free list. It is called exactly once per batch; req must
// not be used afterwards.
func (b *wireBatch) release() {
	if b.buf == nil {
		brokenRule("a batch is released once")
	}
	*b.buf = b.req[:0]
	b.req = nil
	putBatchBuf(b.buf)
	b.buf = nil
}

// traceIDs lists the batch's trace IDs, for recordRefusedIDs when the batch is refused.
func (b *wireBatch) traceIDs() []trace.TraceID {
	ids := make([]trace.TraceID, len(b.spans))
	for i := range b.spans {
		ids[i] = b.spans[i].traceID
	}
	return ids
}

// wireScope is an interned InstrumentationScope: its encoded ScopeSpans.scope field and its
// interning order, which sortByScope groups by. A process has a handful: its server tracer and one
// client tracer per service it calls.
type wireScope struct {
	name, version string // identity, compared by scopeFront lookups
	field         []byte // ScopeSpans.scope field, tag and length included
	order         int    // interning order, from 1
}

// scopeKey is the InstrumentationScope identity at the proto level.
type scopeKey struct{ name, version string }

// wireScopes interns scopes for the whole process. Lookups first scan scopeFront, an immutable slice
// of the first scopes interned (replaced, not modified, under wireScopesMu): the tracer passes the
// same strings every time, so comparing them is a pointer check, much cheaper than hashing a map
// key. Scopes beyond scopeFrontMax are found in the map.
var (
	wireScopes   sync.Map   // scopeKey -> *wireScope
	wireScopesMu sync.Mutex // serializes interning, so orders are unique
	wireScopeN   int        // scopes interned so far
	scopeFront   atomic.Pointer[[]*wireScope]
)

const scopeFrontMax = 32

// internScope returns the interned entry of a scope. It runs for every span; the common case is a
// scan of scopeFront.
func internScope(scope instrumentation.Scope) *wireScope {
	if front := scopeFront.Load(); front != nil {
		for _, ws := range *front {
			if ws.name == scope.Name && ws.version == scope.Version {
				return ws
			}
		}
	}
	key := scopeKey{scope.Name, scope.Version}
	if ws, ok := wireScopes.Load(key); ok {
		return ws.(*wireScope)
	}
	wireScopesMu.Lock()
	defer wireScopesMu.Unlock()
	if ws, ok := wireScopes.Load(key); ok {
		return ws.(*wireScope)
	}
	body, _ := proto.Marshal(&commonpb.InstrumentationScope{Name: scope.Name, Version: scope.Version})
	wireScopeN++
	ws := &wireScope{name: scope.Name, version: scope.Version, field: appendBytesFieldAlways(nil, 1, body), order: wireScopeN} // a set message is written even when empty
	wireScopes.Store(key, ws)
	var front []*wireScope
	if f := scopeFront.Load(); f != nil {
		front = *f
	}
	if len(front) < scopeFrontMax {
		next := append(append([]*wireScope(nil), front...), ws) // a new slice: readers may hold the old one
		scopeFront.Store(&next)
	}
	return ws
}

// scopeField returns the encoded ScopeSpans.scope field of a scope (tests and benchmarks).
func scopeField(scope instrumentation.Scope) []byte { return internScope(scope).field }

// resourceField encodes the ResourceSpans.resource field of a TracerProvider's Resource. Every span
// of a provider shares it, so each processor encodes it once.
func resourceField(res *resource.Resource) []byte {
	var attrs []*commonpb.KeyValue
	if res != nil {
		iter := res.Iter()
		if iter.Len() > 0 {
			attrs = make([]*commonpb.KeyValue, 0, iter.Len())
			for iter.Next() {
				kv := iter.Attribute()
				attrs = append(attrs, &commonpb.KeyValue{Key: string(kv.Key), Value: anyValueProto(kv.Value)})
			}
		}
	}
	body, _ := proto.Marshal(&resourcepb.Resource{Attributes: attrs})
	return appendBytesFieldAlways(nil, 1, body) // a set message is written even when empty
}

// anyValueProto converts an attribute value for the Resource; spans use appendAnyValue.
func anyValueProto(v attribute.Value) *commonpb.AnyValue {
	av := new(commonpb.AnyValue)
	switch v.Type() {
	case attribute.STRING:
		av.Value = &commonpb.AnyValue_StringValue{StringValue: v.AsString()}
	case attribute.INT64:
		av.Value = &commonpb.AnyValue_IntValue{IntValue: v.AsInt64()}
	case attribute.FLOAT64:
		av.Value = &commonpb.AnyValue_DoubleValue{DoubleValue: v.AsFloat64()}
	case attribute.BOOL:
		av.Value = &commonpb.AnyValue_BoolValue{BoolValue: v.AsBool()}
	case attribute.BOOLSLICE:
		vals := make([]*commonpb.AnyValue, 0)
		for _, x := range v.AsBoolSlice() {
			vals = append(vals, &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: x}})
		}
		av.Value = &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}
	case attribute.INT64SLICE:
		vals := make([]*commonpb.AnyValue, 0)
		for _, x := range v.AsInt64Slice() {
			vals = append(vals, &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: x}})
		}
		av.Value = &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}
	case attribute.FLOAT64SLICE:
		vals := make([]*commonpb.AnyValue, 0)
		for _, x := range v.AsFloat64Slice() {
			vals = append(vals, &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: x}})
		}
		av.Value = &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}
	case attribute.STRINGSLICE:
		vals := make([]*commonpb.AnyValue, 0)
		for _, x := range v.AsStringSlice() {
			vals = append(vals, &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: x}})
		}
		av.Value = &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}
	default:
		av.Value = &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", v.AsInterface())}
	}
	return av
}

// buildBatch encodes one ExportTraceServiceRequest: one ResourceSpans holding one ScopeSpans
// holding the given spans, which must all have that scope.
//
//	request       := 0x0a len( resourceSpans )
//	resourceSpans := resourceField || 0x12 len( scopeSpans )
//	scopeSpans    := scopeField || ( 0x12 len( span ) )*
func buildBatch(resource, scope []byte, spans []wireSpan) *wireBatch {
	if len(spans) == 0 {
		brokenInt("a batch has spans", "spans", len(spans), 1)
	}
	ss := len(scope)
	for i := range spans {
		ss += 1 + uvarintLen(uint64(len(spans[i].body))) + len(spans[i].body)
	}
	rs := len(resource) + 1 + uvarintLen(uint64(ss)) + ss
	total := 1 + uvarintLen(uint64(rs)) + rs
	buf := getBatchBuf()
	req := (*buf)[:0]
	if cap(req) < total {
		req = make([]byte, 0, total) // kept by the free list at release, if small enough
	}
	req = append(req, 0x0a)
	req = binary.AppendUvarint(req, uint64(rs))
	req = append(req, resource...)
	req = append(req, 0x12)
	req = binary.AppendUvarint(req, uint64(ss))
	req = append(req, scope...)
	for i := range spans {
		req = appendBytesField(req, 2, spans[i].body)
	}
	if len(req) != total {
		brokenInt("an export request matches its computed size", "bytes", len(req), total)
	}
	return &wireBatch{req: req, n: int64(len(spans)), spans: spans, buf: buf}
}

// sortByScope groups a snapshot by scope in place, with a stable sort by interning order, so each
// scope's spans form one run in their buffered order. A snapshot already in order (a leaf service
// has one scope) is left as it is.
func sortByScope(snap []wireSpan) {
	for i := 1; i < len(snap); i++ {
		if snap[i].scope.order < snap[i-1].scope.order {
			slices.SortStableFunc(snap, func(a, b wireSpan) int { return a.scope.order - b.scope.order })
			return
		}
	}
}

// batches sorts a snapshot by scope and calls emit once per run of at most max spans of one scope,
// each encoded by buildBatch. The batches hold sub-slices of the snapshot, which the caller must not
// reuse.
func batches(resource []byte, snap []wireSpan, max int, emit func(*wireBatch)) {
	if max < 1 {
		brokenInt("a batch holds at least one span", "max", max, 1)
	}
	sortByScope(snap)
	for start := 0; start < len(snap); {
		end := start + 1
		for end < len(snap) && snap[end].scope == snap[start].scope {
			end++
		}
		for s := start; s < end; s += max {
			emit(buildBatch(resource, snap[start].scope.field, snap[s:min(s+max, end)]))
		}
		start = end
	}
}

// --- The span encoder ---

// attrPolicy selects which attributes a processor exports and how its bridge data is written.
type attrPolicy uint8

const (
	policyVanilla    attrPolicy = iota // everything but the response-path keys
	policyPath                         // PB
	policyCallGraph                    // CGPB
	policyStructural                   // SB
)

// encodeSpan writes the OTLP Span message for s. attrs is s.Attributes(); w is the span's wire entry
// (nil for vanilla); highPriority says whether the span is a checkpoint, which decides which bridge
// data it exports.
func encodeSpan(s sdktrace.ReadOnlySpan, attrs []attribute.KeyValue, w *bridgeWire, highPriority bool, policy attrPolicy) wireSpan {
	sc := s.SpanContext()
	tid, sid := sc.TraceID(), sc.SpanID()
	// An ended span has valid IDs; a checkpoint with wire state has a payload, and a reverse context
	// it took over is the packed envelope.
	if !sc.IsValid() {
		brokenRule("an exported span has a valid span context")
	}
	if w != nil {
		if highPriority && len(w.emit) == 0 {
			brokenRule("a checkpoint with wire state has a `_br` payload")
		}
		if w.rev.rc != "" && !backend.IsPackedRetCtx(w.rev.rc) {
			brokenInt("a span's reverse context is a packed envelope", "first byte", int(w.rev.rc[0]), 1)
		}
	}
	size := 64 + len(s.Name()) + 32*len(attrs) + 48*len(s.Events()) + 48*len(s.Links())
	if w != nil {
		size += len(w.emit) + len(w.rev.rc) + 16
	}
	buf := make([]byte, 0, size)
	buf = appendBytesField(buf, 1, tid[:])
	buf = appendBytesField(buf, 2, sid[:])
	if parent := s.Parent(); parent.IsValid() {
		pid := parent.SpanID()
		buf = appendBytesField(buf, 4, pid[:])
	}
	buf = appendStringField(buf, 5, s.Name())
	buf = appendVarintField(buf, 6, uint64(spanKindCode(s.SpanKind())))
	buf = appendFixed64Field(buf, 7, uint64(s.StartTime().UnixNano()))
	buf = appendFixed64Field(buf, 8, uint64(s.EndTime().UnixNano()))
	buf = appendAttributes(buf, 9, attrs, policy, true)
	// A span's bridge data is one attribute (appendBridgeWireBytes).
	if w != nil {
		var scratch [2 * binary.MaxVarintLen64]byte
		buf = appendBridgeWireBytes(buf, w.bridgeBytes(&scratch), highPriority)
	} else if policy != policyVanilla {
		buf = appendBridgeWireBytes(buf, legacyBridgeBytes(attrs, policy), highPriority)
	}
	for _, ev := range s.Events() {
		mark := len(buf)
		buf = appendFixed64Field(buf, 1, uint64(ev.Time.UnixNano()))
		buf = appendStringField(buf, 2, ev.Name)
		buf = appendAttributes(buf, 3, ev.Attributes, policy, false)
		buf = closeMessage(buf, 11, mark)
	}
	for _, link := range s.Links() {
		mark := len(buf)
		ltid, lsid := link.SpanContext.TraceID(), link.SpanContext.SpanID()
		buf = appendBytesField(buf, 1, ltid[:])
		buf = appendBytesField(buf, 2, lsid[:])
		buf = appendAttributes(buf, 4, link.Attributes, policy, false)
		buf = closeMessage(buf, 13, mark)
	}
	// Status is always written, even when empty.
	mark := len(buf)
	st := s.Status()
	buf = appendStringField(buf, 2, st.Description)
	buf = appendVarintField(buf, 3, uint64(st.Code)) // unmapped; see the comment at the top
	buf = closeMessage(buf, 15, mark)
	return wireSpan{body: buf, scope: internScope(s.InstrumentationScope()), traceID: tid}
}

func spanKindCode(kind trace.SpanKind) int {
	switch kind {
	case trace.SpanKindInternal:
		return 1
	case trace.SpanKindServer:
		return 2
	case trace.SpanKindClient:
		return 3
	case trace.SpanKindProducer:
		return 4
	case trace.SpanKindConsumer:
		return 5
	default:
		return 0
	}
}

// appendAttributes writes the attributes the policy lets through as repeated KeyValue messages of the
// given field. Skipped:
//
//	vanilla:  the response-path keys (backend.IsReverseBaggageKey, __bag.__rt_depth)
//	PB:       __bag.*, depth, hasChildren, the ancestry keys
//	CGPB:     as PB, plus childCount
//	SB:       __bag.*, depth, hasChildren, childCount, eventCount, remEndEvents, `_oc`, `d`
//
// span says whether these are the span's own attributes. If so, the bridge keys (`_br`, `_d`, `_o`
// and `_rc`) are left to appendBridgeWireBytes; on events and links they are ordinary attributes.
func appendAttributes(buf []byte, field int, attrs []attribute.KeyValue, policy attrPolicy, span bool) []byte {
	for _, attr := range attrs {
		key := string(attr.Key)
		switch policy {
		case policyVanilla:
			if backend.IsReverseBaggageKey(attr.Key) || key == reverseDepthAttribute {
				continue
			}
		case policyPath, policyCallGraph:
			if strings.HasPrefix(key, "__bag.") {
				continue
			}
			switch key {
			case "depth", "hasChildren", AncestryKey, AncestryModeKey, AncestryExtraKey:
				continue
			case "childCount":
				if policy == policyCallGraph {
					continue
				}
			case AttrBREmit, AttrD, AttrO, backend.ReverseTrussCheckpointKey:
				if span {
					continue
				}
			}
		case policyStructural:
			if strings.HasPrefix(key, "__bag.") {
				continue
			}
			switch key {
			case "depth", "hasChildren", "childCount", "eventCount", "remEndEvents", AttrOC, AttrDepth:
				continue
			case AttrBREmit, AttrD, AttrO, backend.ReverseTrussCheckpointKey:
				if span {
					continue
				}
			}
		}
		mark := len(buf)
		buf = appendStringField(buf, 1, key)
		buf = appendAnyValue(buf, 2, attr.Value)
		buf = closeMessage(buf, field, mark)
	}
	return buf
}

// bridgeBytes is what a span's bridge attribute is made of: from its wire entry, or from the
// attributes the legacy fixed-distance mode uses (legacyBridgeBytes). A nil depth or ordinal means
// the span has none.
type bridgeBytes struct {
	rc         string // reverse context the span took over, raw (or legacy text); "" if none
	emit       []byte // the checkpoint payload
	depth      []byte // uvarint(absolute depth)
	ordinal    []byte // uvarint(child ordinal), SB only
	structural bool
}

// bridgeBytes reads the wire entry; the varints are written into scratch, which the caller owns.
func (w *bridgeWire) bridgeBytes(scratch *[2 * binary.MaxVarintLen64]byte) bridgeBytes {
	n := binary.PutUvarint(scratch[:], uint64(w.depth))
	b := bridgeBytes{rc: w.rev.rc, emit: w.emit, depth: scratch[:n], structural: w.structural}
	if w.structural {
		m := binary.PutUvarint(scratch[n:], uint64(w.ordinal))
		b.ordinal = scratch[n : n+m]
	}
	return b
}

// legacyBridgeBytes collects the bridge data of a span without wire state (fixed-distance mode):
// `_br`, `_d` and `_o` are base64 strings (malformed ones give empty bytes) and `_rc` is the text
// form of the reverse context, converted back to raw.
func legacyBridgeBytes(attrs []attribute.KeyValue, policy attrPolicy) bridgeBytes {
	b := bridgeBytes{structural: policy == policyStructural}
	for _, attr := range attrs {
		switch attr.Key {
		case AttrBREmit:
			b.emit = legacyBytes(attr.Value.AsString())
		case AttrD:
			b.depth = legacyBytes(attr.Value.AsString())
		case AttrO:
			b.ordinal = legacyBytes(attr.Value.AsString())
		case backend.ReverseTrussCheckpointKey:
			b.rc = backend.RetCtxFromText(attr.Value.AsString())
		}
	}
	return b
}

func legacyBytes(b64 string) []byte {
	if raw, ok := decodeBR(b64); ok {
		return raw
	}
	return []byte{}
}

// appendBridgeWireBytes writes a span's bridge data as one attribute:
//
//	checkpoint:          `_br` = uvarint(len(rc)) || rc || payload   (no reverse context: 0x00 || payload)
//	ordinary PB / CGPB:  `_d`  = uvarint(depth)
//	ordinary SB:         `_o`  = uvarint(ordinal) || uvarint(depth)
//
// rc is the packed reverse envelope (backend/reversetruss_packed.go). splitBR reads `_br` back; the
// Python reader is utils/retctx_wire.py.
func appendBridgeWireBytes(buf []byte, b bridgeBytes, highPriority bool) []byte {
	if !highPriority && b.rc != "" {
		// pathCheckpoint and structuralCheckpoint make every span with a reverse context a checkpoint.
		brokenRule("only a checkpoint carries a reverse context")
	}
	if highPriority {
		return appendKeyBR(buf, 9, b.rc, b.emit)
	}
	if b.structural {
		if b.ordinal == nil && b.depth == nil {
			return buf
		}
		return appendKeyBytes2(buf, 9, AttrO, b.ordinal, b.depth)
	}
	if b.depth == nil {
		return buf
	}
	return appendKeyBytes(buf, 9, AttrD, b.depth)
}

// appendKeyBR writes the checkpoint attribute `_br` = uvarint(len(rc)) || rc || emit directly into
// buf.
func appendKeyBR(buf []byte, field int, rc string, emit []byte) []byte {
	mark := len(buf)
	buf = appendStringField(buf, 1, AttrBREmit)
	inner := len(buf)
	buf = append(buf, byte(7<<3|2))
	want := uvarintLen(uint64(len(rc))) + len(rc) + len(emit)
	buf = binary.AppendUvarint(buf, uint64(want))
	value := len(buf)
	buf = binary.AppendUvarint(buf, uint64(len(rc)))
	buf = append(buf, rc...)
	buf = append(buf, emit...)
	if len(buf)-value != want {
		brokenInt("the `_br` bytes value matches its length prefix", "written", len(buf)-value, want)
	}
	buf = closeMessage(buf, 2, inner)
	return closeMessage(buf, field, mark)
}

// appendKeyBytes2 writes one KeyValue whose bytes_value is a || b.
func appendKeyBytes2(buf []byte, field int, key string, a, b []byte) []byte {
	mark := len(buf)
	buf = appendStringField(buf, 1, key)
	inner := len(buf)
	buf = append(buf, byte(7<<3|2))
	buf = binary.AppendUvarint(buf, uint64(len(a)+len(b)))
	buf = append(buf, a...)
	buf = append(buf, b...)
	buf = closeMessage(buf, 2, inner)
	return closeMessage(buf, field, mark)
}

// splitBR splits a `_br` value into the reverse context (nil if none) and the payload. ok is false
// if the length prefix overruns the value.
func splitBR(raw []byte) (rc, emit []byte, ok bool) {
	n, k := binary.Uvarint(raw)
	if k <= 0 || n > uint64(len(raw)-k) {
		return nil, nil, false
	}
	if n > 0 {
		rc = raw[k : k+int(n)]
	}
	return rc, raw[k+int(n):], true
}

// appendKeyBytes writes one KeyValue with a bytes_value, even an empty one (a set oneof member).
func appendKeyBytes(buf []byte, field int, key string, raw []byte) []byte {
	mark := len(buf)
	buf = appendStringField(buf, 1, key)
	inner := len(buf)
	buf = appendBytesFieldAlways(buf, 7, raw)
	buf = closeMessage(buf, 2, inner)
	return closeMessage(buf, field, mark)
}

// appendKeyString writes one KeyValue whose value is the string_value s.
func appendKeyString(buf []byte, field int, key, s string) []byte {
	mark := len(buf)
	buf = appendStringField(buf, 1, key)
	inner := len(buf)
	buf = appendStringFieldAlways(buf, 1, s)
	buf = closeMessage(buf, 2, inner)
	return closeMessage(buf, field, mark)
}

// appendAnyValue writes an AnyValue message as the given field. A oneof member is written even at
// its zero value.
func appendAnyValue(buf []byte, field int, v attribute.Value) []byte {
	mark := len(buf)
	switch v.Type() {
	case attribute.STRING:
		buf = appendStringFieldAlways(buf, 1, v.AsString())
	case attribute.INT64:
		buf = appendVarintFieldAlways(buf, 3, uint64(v.AsInt64()))
	case attribute.FLOAT64:
		buf = appendFixed64FieldAlways(buf, 4, math.Float64bits(v.AsFloat64()))
	case attribute.BOOL:
		var b uint64
		if v.AsBool() {
			b = 1
		}
		buf = appendVarintFieldAlways(buf, 2, b)
	case attribute.BOOLSLICE:
		inner := len(buf)
		for _, x := range v.AsBoolSlice() {
			m := len(buf)
			var b uint64
			if x {
				b = 1
			}
			buf = appendVarintFieldAlways(buf, 2, b)
			buf = closeMessage(buf, 1, m)
		}
		buf = closeMessage(buf, 5, inner)
	case attribute.INT64SLICE:
		inner := len(buf)
		for _, x := range v.AsInt64Slice() {
			m := len(buf)
			buf = appendVarintFieldAlways(buf, 3, uint64(x))
			buf = closeMessage(buf, 1, m)
		}
		buf = closeMessage(buf, 5, inner)
	case attribute.FLOAT64SLICE:
		inner := len(buf)
		for _, x := range v.AsFloat64Slice() {
			m := len(buf)
			buf = appendFixed64FieldAlways(buf, 4, math.Float64bits(x))
			buf = closeMessage(buf, 1, m)
		}
		buf = closeMessage(buf, 5, inner)
	case attribute.STRINGSLICE:
		inner := len(buf)
		for _, x := range v.AsStringSlice() {
			m := len(buf)
			buf = appendStringFieldAlways(buf, 1, x)
			buf = closeMessage(buf, 1, m)
		}
		buf = closeMessage(buf, 5, inner)
	default:
		buf = appendStringFieldAlways(buf, 1, fmt.Sprintf("%v", v.AsInterface()))
	}
	return closeMessage(buf, field, mark)
}

// --- Protobuf primitives. Field numbers are below 16, so every tag is one byte. ---

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// closeMessage turns what was written since mark into a length-prefixed message field, moving the
// body right to make room for the tag and length.
func closeMessage(buf []byte, field int, mark int) []byte {
	if field < 1 || field > 15 {
		brokenInt("a closed message's field number is 1..15", "field", field, 15)
	}
	n := len(buf) - mark
	hdr := 1 + uvarintLen(uint64(n))
	buf = append(buf, make([]byte, hdr)...)
	copy(buf[mark+hdr:], buf[mark:mark+n])
	buf[mark] = byte(field<<3 | 2)
	binary.PutUvarint(buf[mark+1:], uint64(n))
	return buf
}

// Proto3 scalars are omitted at their default value ...
func appendBytesField(buf []byte, field int, b []byte) []byte {
	if len(b) == 0 {
		return buf
	}
	return appendBytesFieldAlways(buf, field, b)
}

func appendStringField(buf []byte, field int, s string) []byte {
	if s == "" {
		return buf
	}
	return appendStringFieldAlways(buf, field, s)
}

func appendVarintField(buf []byte, field int, v uint64) []byte {
	if v == 0 {
		return buf
	}
	return appendVarintFieldAlways(buf, field, v)
}

func appendFixed64Field(buf []byte, field int, v uint64) []byte {
	if v == 0 {
		return buf
	}
	return appendFixed64FieldAlways(buf, field, v)
}

// ... oneof members and set messages are not.
func appendBytesFieldAlways(buf []byte, field int, b []byte) []byte {
	buf = append(buf, byte(field<<3|2))
	buf = binary.AppendUvarint(buf, uint64(len(b)))
	return append(buf, b...)
}

func appendStringFieldAlways(buf []byte, field int, s string) []byte {
	buf = append(buf, byte(field<<3|2))
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func appendVarintFieldAlways(buf []byte, field int, v uint64) []byte {
	buf = append(buf, byte(field<<3|0))
	return binary.AppendUvarint(buf, v)
}

func appendFixed64FieldAlways(buf []byte, field int, v uint64) []byte {
	buf = append(buf, byte(field<<3|1))
	return binary.LittleEndian.AppendUint64(buf, v)
}
