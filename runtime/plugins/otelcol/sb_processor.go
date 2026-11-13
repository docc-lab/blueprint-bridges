package otelcol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/bloom"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataPriorityKey is the gRPC metadata key that marks every export with its priority ("hp" or
// "lp"), so the collector's priority processor can admit or refuse a batch without decoding it.
// It must match priorityprocessor.MetadataPriorityKey.
const MetadataPriorityKey = "bridges-priority"

// Export tuning shared by the bridges. Both priority buffers flush every SBExportInterval; batches
// of at most SBBatchSize spans stay under the receiver's 4 MiB gRPC message limit; the counters
// are logged every SBMetricsInterval.
const (
	SBExportInterval  = 100 * time.Millisecond
	SBBatchSize       = 512
	SBMetricsInterval = 1 * time.Second
)

// sbBufEntry is a buffered span with its scope and priority.
type sbBufEntry struct {
	span         *tracepb.Span
	scope        instrumentation.Scope
	isCheckpoint bool
}

// StructuralBridgeProcessor is the structural bridge (S-Bridge): the CGPB window plus start
// ordinals and end events, enough to reconstruct the order of siblings as well as the call path
// (see OnStart). Export works as for PB.
type StructuralBridgeProcessor struct {
	mu sync.RWMutex

	conn *otlpConn

	agentEndpoint string
	ancestryMode  AncestryMode

	bloomFilter *bloom.BloomFilter

	// Delayed end-event trusses, produced at server OnEnd and attached to the next span this process
	// starts.
	delayedEndEventsChan chan structuralDelayed
	deeOnce              sync.Once

	stopChan chan struct{}
	wg       sync.WaitGroup

	// exportWG tracks in-flight exports, so Shutdown can wait for them before closing the connection.
	exportWG sync.WaitGroup

	// Ended spans, encoded, by priority. flushBuffer sends the high-priority ones first.
	hpBuf      []wireSpan
	lpBuf      []wireSpan
	eventsLock sync.Mutex

	// resource is the encoded Resource, captured on the first OnEnd.
	resource     []byte
	resourceOnce sync.Once

	// Counters, updated atomically. Every flushed span ends up sent or dropped:
	// spansFlushed = spansSent + spansDropped. The send* counters split failed exports by gRPC code.
	spansReceived   int64
	spansFlushed    int64
	spansSent       int64
	batchesSent     int64
	spansDropped    int64
	batchesDropped  int64
	sendDeadline    int64
	sendUnavailable int64
	sendExhausted   int64
	sendCanceled    int64
	sendOther       int64

	// deeDropped counts delayed end events lost because delayedEndEventsChan was full; enqueueing
	// never blocks the request.
	deeDropped int64

	// Counters by priority: checkpoint (cp) or low priority (lp). cpReceived + lpReceived =
	// spansReceived, and likewise for sent and dropped.
	cpReceived int64
	lpReceived int64
	cpSent     int64
	lpSent     int64
	cpDropped  int64
	lpDropped  int64

	// Parameters read from the collector's config discovery endpoint.
	configDiscoveryPort int
	httpClient          *http.Client
	configMap           map[string]interface{}
	configLock          sync.RWMutex

	// checkpointDistance is used in fixed-distance mode; a set checkpointRange selects range mode.
	checkpointDistance int64
	checkpointRange    checkpointRange
	reversePolicy      reversePolicy
}

// NewStructuralBridgeProcessor creates an SB processor. It exports to the collector agent on the host
// of agentEndpoint and reads its parameters from the config discovery endpoint on
// configDiscoveryPort.
func NewStructuralBridgeProcessor(ctx context.Context, agentEndpoint string, configDiscoveryPort string) (*StructuralBridgeProcessor, error) {
	slog.Info("🔵 Creating new StructuralBridgeProcessor", "agentEndpoint", agentEndpoint)

	bloomFilter := bloom.New(BloomFilterM, BloomFilterK)

	var host string
	if strings.Contains(agentEndpoint, ":") {
		parts := strings.Split(agentEndpoint, ":")
		if len(parts) >= 2 {
			host = parts[0]
		}
	}
	if host == "" {
		host = "localhost"
	}

	endpoint := fmt.Sprintf("%s:4317", host)

	slog.Info("🔵 Using endpoint", "endpoint", endpoint)

	// A plain gRPC connection: the processor encodes the OTLP request itself (otlp_wire.go).
	conn, err := dialOTLP(endpoint)
	if err != nil {
		slog.Error("❌ Failed to open the OTLP connection", "error", err)
		return nil, fmt.Errorf("failed to open the OTLP connection: %w", err)
	}
	if grpcRetryEnabled {
		slog.Warn("OTLP_RETRY is not off; the direct OTLP export path does not retry refused batches")
	}

	slog.Info("✅ StructuralBridgeProcessor created successfully")

	configDiscoveryPortInt, err := strconv.Atoi(configDiscoveryPort)
	if err != nil {
		slog.Error("❌ Failed to convert configDiscoveryPort to int", "error", err)
		return nil, fmt.Errorf("failed to convert configDiscoveryPort to int: %w", err)
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	processor := &StructuralBridgeProcessor{
		conn:                 conn,
		agentEndpoint:        agentEndpoint,
		bloomFilter:          bloomFilter,
		stopChan:             make(chan struct{}),
		ancestryMode:         AncestryModePB,
		configDiscoveryPort:  configDiscoveryPortInt,
		httpClient:           httpClient,
		configMap:            make(map[string]interface{}),
		checkpointDistance:   1, // until the config says otherwise
		delayedEndEventsChan: make(chan structuralDelayed, 10000),
	}

	slog.Info("🔵 Ancestry mode configured", "mode", AncestryModePB)

	slog.Info("🔵 About to fetch full config")
	if err := processor.fetchFullConfig(ctx); err != nil {
		if errors.Is(err, errInvalidCheckpointRange) || errors.Is(err, errInvalidReversePolicy) {
			_ = conn.close()
			return nil, err
		}
		slog.Error("❌ Failed to fetch full config", "error", err)
		slog.Warn("⚠️ Continuing with empty config map")
	} else {
		slog.Info("🟢 Successfully fetched full config", "config_keys", len(processor.configMap))
	}

	processor.wg.Add(1)
	go processor.processEvents()

	processor.wg.Add(1)
	go processor.logMetricsLoop()

	return processor, nil
}

// logMetricsLoop logs the counters every SBMetricsInterval until stopChan is closed.
func (p *StructuralBridgeProcessor) logMetricsLoop() {
	defer p.wg.Done()
	t := time.NewTicker(SBMetricsInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopChan:
			return
		case <-t.C:
			p.logMetrics()
		}
	}
}

// logMetrics logs every counter and the buffer depths.
func (p *StructuralBridgeProcessor) logMetrics() {
	p.eventsLock.Lock()
	hpDepth := len(p.hpBuf)
	lpDepth := len(p.lpBuf)
	p.eventsLock.Unlock()
	// Loaded later-incremented first, for the order checks (checkExportCounts).
	cpSent, lpSent := atomic.LoadInt64(&p.cpSent), atomic.LoadInt64(&p.lpSent)
	cpDropped, lpDropped := atomic.LoadInt64(&p.cpDropped), atomic.LoadInt64(&p.lpDropped)
	sent, dropped := atomic.LoadInt64(&p.spansSent), atomic.LoadInt64(&p.spansDropped)
	flushed := atomic.LoadInt64(&p.spansFlushed)
	cpReceived, lpReceived := atomic.LoadInt64(&p.cpReceived), atomic.LoadInt64(&p.lpReceived)
	received := atomic.LoadInt64(&p.spansReceived)
	checkExportCounts(sent, dropped, flushed, received)
	checkPriorityCounts("sent", cpSent, lpSent, sent)
	checkPriorityCounts("dropped", cpDropped, lpDropped, dropped)
	checkPriorityCounts("received", cpReceived, lpReceived, received)
	slog.Info("sb_processor_metrics",
		"spans_received", received,
		"spans_flushed", flushed,
		"spans_sent", sent,
		"batches_sent", atomic.LoadInt64(&p.batchesSent),
		"spans_dropped", dropped,
		"batches_dropped", atomic.LoadInt64(&p.batchesDropped),
		"cp_received", cpReceived,
		"lp_received", lpReceived,
		"cp_sent", cpSent,
		"lp_sent", lpSent,
		"cp_dropped", cpDropped,
		"lp_dropped", lpDropped,
		"send_deadline", atomic.LoadInt64(&p.sendDeadline),
		"send_unavailable", atomic.LoadInt64(&p.sendUnavailable),
		"send_exhausted", atomic.LoadInt64(&p.sendExhausted),
		"send_canceled", atomic.LoadInt64(&p.sendCanceled),
		"send_other", atomic.LoadInt64(&p.sendOther),
		"hp_buffer_depth", hpDepth,
		"lp_buffer_depth", lpDepth,
		"dee_dropped", atomic.LoadInt64(&p.deeDropped),
		"dee_chan_depth", len(p.delayedEndEventsChan),
	)
	// The retry gauges go on a line of their own.
	sdkRetry.logMetrics()
}

// categorizeSendError counts a failed export under its gRPC status code.
func (p *StructuralBridgeProcessor) categorizeSendError(err error) {
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		atomic.AddInt64(&p.sendDeadline, 1)
	case codes.Unavailable:
		atomic.AddInt64(&p.sendUnavailable, 1)
	case codes.ResourceExhausted:
		atomic.AddInt64(&p.sendExhausted, 1)
	case codes.Canceled:
		atomic.AddInt64(&p.sendCanceled, 1)
	default:
		atomic.AddInt64(&p.sendOther, 1)
	}
}

// processEvents flushes the buffers every SBExportInterval, and once more when stopped.
func (p *StructuralBridgeProcessor) processEvents() {
	defer p.wg.Done()

	ticker := time.NewTicker(SBExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			// Shutdown waits for the exports this starts (exportWG).
			p.flushBuffer()
			return
		case <-ticker.C:
			// Tracked like the exports it starts, so Shutdown waits for it too.
			p.exportWG.Add(1)
			go func() {
				defer p.exportWG.Done()
				p.flushBuffer()
			}()
		}
	}
}

// flushBuffer exports both buffers, high priority first, so high-priority batches tend to reach the
// collector first within a flush. Each batch holds one priority and says which in the
// bridges-priority gRPC metadata.
func (p *StructuralBridgeProcessor) flushBuffer() {
	p.eventsLock.Lock()
	hpSnap := p.hpBuf
	lpSnap := p.lpBuf
	p.hpBuf = make([]wireSpan, 0, cap(p.hpBuf))
	p.lpBuf = make([]wireSpan, 0, cap(p.lpBuf))
	p.eventsLock.Unlock()

	if len(hpSnap) == 0 && len(lpSnap) == 0 {
		return
	}

	atomic.AddInt64(&p.spansFlushed, int64(len(hpSnap)+len(lpSnap)))

	p.dispatchPriority(hpSnap, true)
	p.dispatchPriority(lpSnap, false)
}

// dispatchPriority exports one priority's spans in batches of at most SBBatchSize, each in its own
// goroutine.
func (p *StructuralBridgeProcessor) dispatchPriority(snap []wireSpan, isHP bool) {
	if len(snap) == 0 {
		return
	}
	batches(p.resource, snap, SBBatchSize, func(batch *wireBatch) {
		p.exportWG.Add(1)
		go p.exportBatch(batch, isHP)
	})
}

// exportBatch sends one batch and counts the outcome. A refused batch is dropped, or retried once if
// the retry policy says so (sdk_retry.go).
func (p *StructuralBridgeProcessor) exportBatch(batch *wireBatch, isHP bool) {
	defer p.exportWG.Done()
	// At most one retry, per BRIDGES_RETRY (default off).
	if !isHP && sdkRetry.suppressLP() {
		sdkRetry.lpSuppressed.Add(batch.n)
		p.batchDropped(batch, isHP, errLPSuppressed)
		batch.release()
		return
	}
	err := p.sendData(batch.req, isHP)
	if err != nil && sdkRetry.wants(isHP, err) {
		p.exportWG.Add(1)
		sdkRetry.scheduleBytes(int64(len(batch.req)), batch.n, func() error { return p.sendData(batch.req, isHP) }, func(err error) {
			defer p.exportWG.Done()
			defer batch.release()
			if err != nil {
				p.batchDropped(batch, isHP, err)
				return
			}
			p.batchSent(batch.n, isHP)
		})
		return
	}
	defer batch.release()
	if err != nil {
		p.batchDropped(batch, isHP, err)
		return
	}
	p.batchSent(batch.n, isHP)
}

// batchDropped counts a batch that will not be delivered and records its trace IDs.
func (p *StructuralBridgeProcessor) batchDropped(batch *wireBatch, isHP bool, err error) {
	atomic.AddInt64(&p.batchesDropped, 1)
	atomic.AddInt64(&p.spansDropped, batch.n)
	if isHP {
		atomic.AddInt64(&p.cpDropped, batch.n)
	} else {
		atomic.AddInt64(&p.lpDropped, batch.n)
	}
	p.categorizeSendError(err)
	recordRefusedIDs(batch.traceIDs(), isHP)
	slog.Error("Failed to send batch", "error", err, "count", batch.n, "hp", isHP)
}

// batchSent counts a delivered batch.
func (p *StructuralBridgeProcessor) batchSent(n int64, isHP bool) {
	atomic.AddInt64(&p.batchesSent, 1)
	atomic.AddInt64(&p.spansSent, n)
	if isHP {
		atomic.AddInt64(&p.cpSent, n)
	} else {
		atomic.AddInt64(&p.lpSent, n)
	}
	slog.Debug("Sent batch", "count", n, "hp", isHP)
}

// sendData exports one batch, marked with its priority in the bridges-priority gRPC metadata so the
// collector's priority processor can admit or refuse it without decoding it.
func (p *StructuralBridgeProcessor) sendData(req []byte, isHP bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), grpcDeadline)
	defer cancel()

	priorityVal := "lp"
	if isHP {
		priorityVal = "hp"
	}
	ctx = metadata.AppendToOutgoingContext(ctx, MetadataPriorityKey, priorityVal)

	if err := p.conn.export(ctx, req); err != nil {
		slog.Error("❌ Failed to send data", "error", err, "hp", isHP)
		return fmt.Errorf("failed to send data: %w", err)
	}
	return nil
}

// startAttributeBaggage reports whether OnStart adds "__bag." attributes. SB keeps its state in the
// wire table and adds at most "padding".
func (p *StructuralBridgeProcessor) startAttributeBaggage() bool { return false }

// OnStart implements [sdktrace.SpanProcessor].
//
// The structural bridge (SB) is the CGPB window (anchor, depth, Bloom filter, fan-out witness) plus
// each span's start ordinal and its parent's end events, so reconstruction can also order
// siblings. The wire layout is in structural_truss.go.
//
// OnStart works in two steps: structuralInputsFrom does the context lookups, which walk a deep
// chain, in a small stack frame; startStructural then does the window work, which needs a large
// frame but calls nothing deep. Doing both in one function made new goroutines grow their stacks.
func (p *StructuralBridgeProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	in := structuralInputsFrom(parent)
	p.startStructural(s, &in)
	if spanPadding != "" {
		s.SetAttributes(attribute.String("padding", spanPadding))
	}
}

// structuralInputs is what a span's window needs from its parent's context.
type structuralInputs struct {
	br       string       // the parent's outgoing `_br`, "" if none
	seqNum   int          // start ordinal among its siblings
	ends     []int        // siblings that had ended when it started, in end order (a copy)
	parentID trace.SpanID // the parent, for the fan-out witness of the second child
}

// structuralInputsFrom reads the window's inputs from the parent's context.
func structuralInputsFrom(parent context.Context) (in structuralInputs) {
	if bag := backend.GetBaggageFromContext(parent); bag != nil {
		in.br = bag[BaggageBRKey]
	}
	if v := parent.Value("seqNum"); v != nil {
		var isInt bool
		if in.seqNum, isInt = v.(int); !isInt || in.seqNum < 1 {
			brokenInt("a span's child ordinal (seqNum) is an int >= 1", "seqNum", in.seqNum, 1)
		}
	}
	if in.seqNum == 2 {
		in.parentID = trace.SpanFromContext(parent).SpanContext().SpanID()
	}
	// The parent's end events so far: start ordinals of the siblings that have ended, in end order.
	if eePtr, ok := parent.Value("endEvents").(*[]int); ok && eePtr != nil {
		mu, _ := parent.Value("childrenMutex").(*sync.Mutex)
		if mu != nil {
			mu.Lock()
		}
		if len(*eePtr) > 0 {
			in.ends = append([]int(nil), (*eePtr)...)
		}
		if mu != nil {
			mu.Unlock()
		}
	}
	return in
}

// startStructural computes the span's window and stores its export payload and outgoing `_br` in
// the wire table. The parent's tail is not decoded: the child's tail is the parent's bytes plus
// one entry (appendChildTail), built once and shared by both payloads, which differ only in the
// window core (the export payload has the filter from before this span was added).
func (p *StructuralBridgeProcessor) startStructural(s sdktrace.ReadWriteSpan, in *structuralInputs) {
	var (
		hasParent     bool
		parentTTL     byte
		parentDepth   int
		anchor        [8]byte
		distance      int
		inherited     []byte
		parentWitness []byte
		tail          rawStructuralTail
	)
	if raw := viewPropBR(in.br); raw != nil { // read-only
		parentTTL, raw = p.checkpointRange.unwrap(raw)
		var ok bool
		parentDepth, anchor, distance, inherited, parentWitness, tail, ok = scanStructuralBR(raw)
		// The parent's `_br` was written by this bridge, so it must scan, it carries at most one
		// fan-out witness, and in range mode its TTL must be below its distance.
		if !ok {
			brokenRule("the parent's `_br` scans as an S-Bridge window")
		}
		if len(parentWitness) != 0 {
			checkParentWitness(parentWitness, parentDepth)
		}
		if p.checkpointRange.enabled() && int(parentTTL) >= distance {
			brokenInt("a window's TTL is below its distance", "ttl", int(parentTTL), distance)
		}
		hasParent = true
		if in.seqNum < 1 { // a child of a bridge span gets its ordinal from the parent
			brokenInt("an S-Bridge span with a parent window has a child ordinal >= 1", "seqNum", in.seqNum, 1)
		}
	}
	cpd := int(p.checkpointDistance)
	if cpd < 1 {
		cpd = 1
	}
	depth := 0
	if hasParent {
		depth = parentDepth + 1
	}
	var isCheckpoint bool
	var outgoingTTL byte
	if p.checkpointRange.enabled() {
		isCheckpoint, outgoingTTL = p.checkpointRange.next(parentTTL, hasParent)
	} else {
		// Fixed distance: the same window layout, with checkpoints at fixed depths.
		isCheckpoint, distance = depth%cpd == 0, min(cpd, 256)
	}
	if !hasParent {
		anchor = [8]byte{}
		if p.checkpointRange.enabled() {
			distance = int(outgoingTTL) + 1
		}
		inherited, parentWitness, tail = nil, nil, rawStructuralTail{}
	}
	if distance < 1 || distance > len(checkpointBlooms) {
		brokenInt("a window's distance is 1..256", "distance", distance, len(checkpointBlooms))
	}
	geometry := checkpointBlooms[distance-1]
	if inherited != nil && len(inherited) != geometry.bytes {
		brokenInt("an inherited Bloom has its distance's width", "bytes", len(inherited), geometry.bytes)
	}
	filter := bloom.NewFromBytes(inherited, geometry.m, geometry.k)

	// The fan-out witness, as in CGPB: the second child gets a new one, the first inherits the
	// parent's, later children get none.
	var witness []byte
	if hasParent && in.seqNum == 1 {
		witness = append([]byte(nil), parentWitness...)
	} else if hasParent && in.seqNum == 2 {
		witness = newFanoutWitness(in.parentID, depth)
	}

	// Delayed end-event trusses left by earlier requests in this process ride on this span until a
	// checkpoint exports them.
	var drained []structuralDelayed
	queue := p.delayedQueue()
	for draining := true; draining; {
		select {
		case entry := <-queue:
			drained = append(drained, entry)
		default:
			draining = false
		}
	}

	// Exported by checkpoints and leaves: the inherited window plus this span's ordinal and end-event
	// group.
	headerLen := varintLen(depth) + 9 + geometry.bytes + varintLen(len(witness)) + len(witness)
	tailCap := 24 + len(tail.entries) + len(tail.delayed) + (in.seqNum+7)/8 + 4*len(in.ends) + 48*len(drained)
	emit := appendStructuralHeader(make([]byte, 0, headerLen+tailCap), depth, anchor, distance, filter.Bytes(), witness)
	tailStart := len(emit)
	emit = appendChildTail(emit, tail, in.seqNum, in.ends, drained)
	childTail := emit[tailStart:]

	// What the children inherit, written with the range mode's TTL byte in front.
	sid := s.SpanContext().SpanID()
	var prop []byte
	if p.checkpointRange.enabled() {
		prop = append(make([]byte, 0, 1+headerLen+len(childTail)), outgoingTTL)
	}
	if isCheckpoint {
		// A checkpoint exports everything above; its children start an empty window anchored on it.
		if p.checkpointRange.enabled() {
			distance = int(outgoingTTL) + 1
			geometry = checkpointBlooms[distance-1]
		}
		prop = binary.AppendUvarint(prop, uint64(maxInt(depth, 0)))
		prop = append(prop, sid[:]...)
		prop = append(prop, byte(distance-1))
		prop = append(prop, make([]byte, geometry.bytes)...) // empty filter
		prop = append(prop, 0, 0, 0)                         // no witness, no entries, no delayed entries
	} else {
		filter.AddPrehashed(sid[:])
		prop = appendStructuralHeader(prop, depth, anchor, distance, filter.Bytes(), witness)
		prop = append(prop, childTail...)
	}

	// Nothing goes on the OTel span itself: the instrumentation picks up the outgoing `_br` through
	// backend.AppendSpanBaggage, and the rest stays in the wire table.
	bridgeWires.store(sid, &bridgeWire{emit: emit, depth: depth, priority: isCheckpoint, structural: true, ordinal: in.seqNum,
		prop: propBROwned(prop)})
}

// delayedQueue returns the delayed-truss queue, creating it for processors built without the
// constructor (tests).
func (p *StructuralBridgeProcessor) delayedQueue() chan structuralDelayed {
	p.deeOnce.Do(func() {
		if p.delayedEndEventsChan == nil {
			p.delayedEndEventsChan = make(chan structuralDelayed, 10000)
		}
	})
	return p.delayedEndEventsChan
}

// OnEnd implements [sdktrace.SpanProcessor]. A server span's instrumentation records the end
// events of its children after the last one started (remEndEvents); they become a delayed truss,
// stamped with this span's IDs, for the next span this process starts. A span is a checkpoint by
// the PB/CGPB rule: a scheduled checkpoint, or a server span with no children, unless the
// response path rejected it.
func (p *StructuralBridgeProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	// One Attributes call serves the end-event summary, classification and encoding. The wire entry is
	// taken (and freed) here.
	attrs := s.Attributes()
	w := bridgeWires.take(s.SpanContext().SpanID())
	if w == nil {
		brokenRule("an S-Bridge span ends with the wire entry its OnStart stored")
	}
	for _, attr := range attrs {
		if attr.Key != "remEndEvents" {
			continue
		}
		// Raw bytes, as the instrumentation writes them.
		if summary := []byte(attr.Value.AsString()); len(summary) > 0 {
			d, valid := structuralDelayedFromServer(s.SpanContext().TraceID(), s.SpanContext().SpanID(), summary)
			if !valid { // written by this process's server instrumentation
				brokenRule("a remEndEvents summary decodes")
			}
			if len(d.ends) == 0 {
				continue
			}
			select {
			case p.delayedQueue() <- d:
			default:
				atomic.AddInt64(&p.deeDropped, 1)
			}
		}
	}
	// Apply the response path's decisions; the delayed queue is not affected.
	p.routeToPipeline(s, attrs, w, structuralCheckpoint(s.SpanKind(), attrs, w))
}

// routeToPipeline counts the span, encodes it and buffers it by priority. The exported `_br` is
// what marks a span high priority for the collector.
func (p *StructuralBridgeProcessor) routeToPipeline(s sdktrace.ReadOnlySpan, attrs []attribute.KeyValue, w *bridgeWire, highPriority bool) {
	atomic.AddInt64(&p.spansReceived, 1)
	if highPriority {
		atomic.AddInt64(&p.cpReceived, 1)
	} else {
		atomic.AddInt64(&p.lpReceived, 1)
	}
	p.resourceOnce.Do(func() {
		p.resource = resourceField(s.Resource())
	})
	// `_br`, `_o` and `_d` come from the wire entry; policyStructural drops the internal attributes.
	entry := encodeSpan(s, attrs, w, highPriority, policyStructural)
	p.eventsLock.Lock()
	if highPriority {
		p.hpBuf = append(p.hpBuf, entry)
	} else {
		p.lpBuf = append(p.lpBuf, entry)
	}
	hpSize := len(p.hpBuf)
	lpSize := len(p.lpBuf)
	p.eventsLock.Unlock()
	slog.Debug("🔴 Routed to pipeline", "span_name", s.Name(), "hp_buffer_size", hpSize, "lp_buffer_size", lpSize, "high_prio", highPriority)
}

// getConfigDiscoveryEndpoint is the address of the collector's config discovery endpoint: the
// agent's host, on the discovery port.
func (p *StructuralBridgeProcessor) getConfigDiscoveryEndpoint() string {
	if strings.Contains(p.agentEndpoint, ":") {
		parts := strings.Split(p.agentEndpoint, ":")
		if len(parts) >= 2 {
			host := parts[0]
			return fmt.Sprintf("%s:%d", host, p.configDiscoveryPort)
		}
	}
	return fmt.Sprintf("localhost:%d", p.configDiscoveryPort)
}

// parseCheckpointDistance reads the fixed checkpoint distance (cpd) from the config.
func (p *StructuralBridgeProcessor) parseCheckpointDistance(config map[string]interface{}) int64 {
	const defaultCPD = int64(1) // every span is a checkpoint

	if config == nil {
		return defaultCPD
	}

	if cpdVal, exists := config["cpd"]; exists {
		// JSON numbers arrive as float64; accept ints and strings too.
		switch v := cpdVal.(type) {
		case int64:
			if v > 0 {
				return v
			}
			slog.Warn("cpd must be positive, using default", "cpd", v)
			return defaultCPD
		case int:
			if v > 0 {
				return int64(v)
			}
			slog.Warn("cpd must be positive, using default", "cpd", v)
			return defaultCPD
		case float64:
			cpd := int64(v)
			if cpd > 0 && float64(cpd) == v {
				return cpd
			}
			slog.Warn("cpd must be a positive integer, using default", "cpd", v)
			return defaultCPD
		case string:
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				if parsed > 0 {
					return parsed
				}
				slog.Warn("cpd must be positive, using default", "cpd", v)
				return defaultCPD
			}
			slog.Warn("Failed to parse cpd as int64, using default", "cpd", v, "error", err)
			return defaultCPD
		default:
			slog.Warn("cpd has unexpected type, using default", "cpd", v, "type", fmt.Sprintf("%T", v))
			return defaultCPD
		}
	}

	return defaultCPD
}

// fetchFullConfig reads the parameters from the config discovery endpoint and applies them.
func (p *StructuralBridgeProcessor) fetchFullConfig(ctx context.Context) error {
	config, err := p.fetchFullConfigFromEndpointWithRetries(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch full config: %w", err)
	}

	cpd := p.parseCheckpointDistance(config)
	// Validate everything before applying anything.
	policy, err := parseReversePolicy(config)
	if err != nil {
		return err
	}
	rangeConfig, err := parseCheckpointRange(config)
	if err != nil {
		return err
	}
	if rangeConfig.enabled() {
		cpd = int64(rangeConfig.max)
	}

	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		slog.Warn("Failed to marshal config to JSON for logging", "error", err)
	} else {
		slog.Info("Fetched config JSON", "config", string(configJSON))
	}

	p.configLock.Lock()
	p.configMap = config
	p.checkpointDistance = cpd
	p.checkpointRange = rangeConfig
	p.reversePolicy = policy
	p.configLock.Unlock()

	slog.Info("Successfully discovered full config",
		"config_keys", len(config),
		"checkpoint_distance", cpd,
		"cpd_min", rangeConfig.min, "cpd_max", rangeConfig.max,
		"reverse_policy", policy.mode, "reverse_probability", policy.probability,
		"reverse_passthrough", policy.passthrough)
	return nil
}

// fetchFullConfigFromEndpointWithRetries fetches the config, retrying once a second for up to a
// minute while the collector is not up yet.
func (p *StructuralBridgeProcessor) fetchFullConfigFromEndpointWithRetries(ctx context.Context) (map[string]interface{}, error) {
	configDiscoveryEndpoint := p.getConfigDiscoveryEndpoint()
	url := fmt.Sprintf("http://%s/getFullConfig", configDiscoveryEndpoint)

	for attempt := 1; attempt <= 60; attempt++ {
		slog.Debug("Attempting config discovery", "attempt", attempt, "endpoint", url)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP request: %w", err)
		}

		resp, err := p.httpClient.Do(req)
		if err != nil {
			slog.Debug("Config discovery attempt failed, will retry", "attempt", attempt, "error", err)
			if attempt < 30 {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("failed to make HTTP request after %d attempts: %w", attempt, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			slog.Debug("Config discovery endpoint returned non-OK status, will retry", "attempt", attempt, "status", resp.StatusCode)
			if attempt < 30 {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("config discovery endpoint returned status %d after %d attempts", resp.StatusCode, attempt)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			slog.Debug("Failed to read response body, will retry", "attempt", attempt, "error", err)
			if attempt < 30 {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("failed to read response body after %d attempts: %w", attempt, err)
		}

		var configResp ConfigResponse
		if err := json.Unmarshal(body, &configResp); err != nil {
			slog.Debug("Failed to parse config response, will retry", "attempt", attempt, "error", err)
			if attempt < 30 {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("failed to parse config response after %d attempts: %w", attempt, err)
		}

		if configResp.Config == nil {
			slog.Debug("Empty config in response, will retry", "attempt", attempt)
			if attempt < 30 {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("empty config in response after %d attempts", attempt)
		}

		slog.Info("Config discovery successful", "attempt", attempt, "config_keys", len(configResp.Config))
		return configResp.Config, nil
	}

	return nil, fmt.Errorf("config discovery failed after 60 attempts")
}

// getConfigMap returns a copy of the config map.
func (p *StructuralBridgeProcessor) getConfigMap() map[string]interface{} {
	p.configLock.RLock()
	defer p.configLock.RUnlock()

	result := make(map[string]interface{})
	for k, v := range p.configMap {
		result[k] = v
	}
	return result
}

// Shutdown implements [sdktrace.SpanProcessor]. It flushes the buffers and waits for every export.
func (p *StructuralBridgeProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Info("🔴 StructuralBridgeProcessor shutting down")

	// processEvents flushes one last time.
	close(p.stopChan)
	p.wg.Wait()

	p.exportWG.Wait()
	checkDrainedCounts(atomic.LoadInt64(&p.spansSent), atomic.LoadInt64(&p.spansDropped), atomic.LoadInt64(&p.spansFlushed))

	if err := p.conn.close(); err != nil {
		slog.Error("❌ Failed to stop client", "error", err)
	}

	slog.Info("🔴 StructuralBridgeProcessor shutdown complete — final metrics:")
	p.logMetrics()
	return nil
}

// ForceFlush implements [sdktrace.SpanProcessor]. It does nothing: spans are exported every
// SBExportInterval.
func (p *StructuralBridgeProcessor) ForceFlush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return nil
}
