package otelcol

import (
	"context"
	"encoding/json"
	"fmt"
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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// CallGraphBridgeProcessor is the call-graph bridge (CG-Bridge): PB plus at most one fan-out witness
// per truss, which records where the call path branched, so reconstruction can also place the
// siblings in a window (see newFanoutWitness). Export works exactly as for PB.
type CallGraphBridgeProcessor struct {
	mu sync.RWMutex

	conn *otlpConn

	agentEndpoint string
	ancestryMode  AncestryMode

	bloomFilter *bloom.BloomFilter

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

	// Counters, updated atomically; see PathBridgeProcessor.
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
	cpReceived      int64
	lpReceived      int64
	cpSent          int64
	lpSent          int64
	cpDropped       int64
	lpDropped       int64

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

// NewCallGraphBridgeProcessor creates a CGPB processor. It exports to the collector agent on the host
// of agentEndpoint and reads its parameters from the config discovery endpoint on
// configDiscoveryPort.
func NewCallGraphBridgeProcessor(ctx context.Context, agentEndpoint string, configDiscoveryPort string) (*CallGraphBridgeProcessor, error) {
	slog.Info("🔵 Creating new CallGraphBridgeProcessor", "agentEndpoint", agentEndpoint)

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

	slog.Info("✅ CallGraphBridgeProcessor created successfully")

	configDiscoveryPortInt, err := strconv.Atoi(configDiscoveryPort)
	if err != nil {
		slog.Error("❌ Failed to convert configDiscoveryPort to int", "error", err)
		return nil, fmt.Errorf("failed to convert configDiscoveryPort to int: %w", err)
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	processor := &CallGraphBridgeProcessor{
		conn:                conn,
		agentEndpoint:       agentEndpoint,
		bloomFilter:         bloom.New(BloomFilterM, BloomFilterK),
		stopChan:            make(chan struct{}),
		ancestryMode:        AncestryModePB,
		configDiscoveryPort: configDiscoveryPortInt,
		httpClient:          httpClient,
		configMap:           make(map[string]interface{}),
		checkpointDistance:  1, // until the config says otherwise
	}

	slog.Info("🔵 Ancestry mode configured", "mode", AncestryModePB)

	// Bloom geometry for the default distance; fetchFullConfig resizes it for the configured one.
	expectedElements := uint(pbBloomCapacity(int(processor.checkpointDistance)))
	if expectedElements == 0 {
		expectedElements = 1
	}
	calculatedM, calculatedK := bloom.EstimateParameters(expectedElements, DefaultBloomFPRate)
	BloomFilterM = calculatedM
	BloomFilterK = calculatedK
	backend.SetRTBloomParams(calculatedM, calculatedK)

	slog.Info("🔵 About to fetch full config")
	if err := processor.fetchFullConfig(ctx); err != nil {
		_ = conn.close()
		return nil, err
	}
	slog.Info("🟢 Successfully fetched full config", "config_keys", len(processor.configMap))

	processor.wg.Add(1)
	go processor.processEvents()

	processor.wg.Add(1)
	go processor.logMetricsLoop()

	return processor, nil
}

// logMetricsLoop logs the counters every SBMetricsInterval until stopChan is closed.
func (p *CallGraphBridgeProcessor) logMetricsLoop() {
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
func (p *CallGraphBridgeProcessor) logMetrics() {
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
	// The configuration in force goes on the line too; the experiment runner checks it before
	// every load run (utils/run_dsb_sn_nw.py, check_sdk_config).
	p.configLock.RLock()
	cpd, rng := p.checkpointDistance, p.checkpointRange
	p.configLock.RUnlock()
	slog.Info("cgpb_processor_metrics",
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
		"cpd", cpd,
		"cpd_min", rng.min,
		"cpd_max", rng.max,
	)
	// The retry gauges go on a line of their own.
	sdkRetry.logMetrics()
}

// categorizeSendError counts a failed export under its gRPC status code.
func (p *CallGraphBridgeProcessor) categorizeSendError(err error) {
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
func (p *CallGraphBridgeProcessor) processEvents() {
	defer p.wg.Done()

	ticker := time.NewTicker(SBExportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
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

// flushBuffer exports both buffers, high priority first. Each batch holds one priority and says which
// in the bridges-priority gRPC metadata.
func (p *CallGraphBridgeProcessor) flushBuffer() {
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
func (p *CallGraphBridgeProcessor) dispatchPriority(snap []wireSpan, isHP bool) {
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
func (p *CallGraphBridgeProcessor) exportBatch(batch *wireBatch, isHP bool) {
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
func (p *CallGraphBridgeProcessor) batchDropped(batch *wireBatch, isHP bool, err error) {
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
func (p *CallGraphBridgeProcessor) batchSent(n int64, isHP bool) {
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
func (p *CallGraphBridgeProcessor) sendData(req []byte, isHP bool) error {
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

// startAttributeBaggage reports whether OnStart adds "__bag." attributes: only the legacy
// fixed-distance mode does.
func (p *CallGraphBridgeProcessor) startAttributeBaggage() bool { return !p.checkpointRange.enabled() }

// OnStart implements [sdktrace.SpanProcessor].
func (p *CallGraphBridgeProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	if p.checkpointRange.enabled() {
		onStartCheckpointWindow(parent, s, p.checkpointRange, true)
		return
	}
	p.onStartFixed(parent, s)
}

// onStartFixed is OnStart in the legacy fixed-distance mode. It is a separate function so that
// OnStart's stack frame stays small in range mode.
func (p *CallGraphBridgeProcessor) onStartFixed(parent context.Context, s sdktrace.ReadWriteSpan) {
	// The parent's `_br` is varint(depth) || ckpt(8) || bloom || witness; the bloom has a fixed width.
	bloomLen := int((BloomFilterM + 7) / 8)
	var (
		hasParent        bool
		parentDepth      int
		parentCkpt       [8]byte
		parentBloomBytes []byte
		parentWitness    []byte
	)
	if baggage := backend.GetBaggageFromContext(parent); baggage != nil {
		if br, ok := baggage[BaggageBRKey]; ok && br != "" {
			if raw, okB := decodeBR(br); okB {
				if d, c4, bb, wb, okU := unpackCGPRBBR(raw, bloomLen); okU {
					hasParent = true
					parentDepth = d
					parentCkpt = c4
					parentBloomBytes = bb
					parentWitness = wb
				}
			}
		}
	}
	if len(parentWitness) != 0 {
		checkParentWitness(parentWitness, parentDepth)
	}

	cpd := int(p.checkpointDistance)
	if cpd < 1 {
		cpd = 1
	}

	// The window inherited from the parent.
	var depth int
	var inheritedCkpt [8]byte
	var bf *bloom.BloomFilter
	if hasParent {
		depth = parentDepth + 1
		inheritedCkpt = parentCkpt
		bf = bloom.NewFromBytes(parentBloomBytes, BloomFilterM, BloomFilterK)
	} else {
		depth = 0
		bf = bloom.New(BloomFilterM, BloomFilterK)
	}

	// The fan-out witness: the second child gets a new one, the first inherits the parent's, later
	// children get none. seqNum is the child's ordinal, set by the instrumentation.
	seqNum, _ := parent.Value("seqNum").(int)
	var witness []byte
	if hasParent && seqNum == 1 {
		witness = append([]byte(nil), parentWitness...)
	} else if hasParent && seqNum == 2 {
		witness = newFanoutWitness(trace.SpanFromContext(parent).SpanContext().SpanID(), depth)
	}

	isCheckpoint := depth%cpd == 0

	spanID := s.SpanContext().SpanID().String()
	sid8, _ := spanIDHexTo8Bytes(spanID)
	var ownCkpt [8]byte
	copy(ownCkpt[:], sid8[:])

	// Checkpoints and leaves export the inherited window and their witness.
	emitPayload := packCGPRBBR(depth, inheritedCkpt, bf.Bytes(), witness)

	// What the children inherit. A checkpoint anchors a new window on itself, with an empty filter
	// and no witness; any other span adds itself to the filter and passes its witness on.
	priority := 0
	var propCkpt [8]byte
	var propBloomBytes []byte
	propWitness := witness
	if isCheckpoint {
		priority = 1
		propCkpt = ownCkpt
		propBloomBytes = bloom.New(BloomFilterM, BloomFilterK).Bytes()
		propWitness = nil
	} else {
		propCkpt = inheritedCkpt
		// Span IDs are random, so they serve as the filter's hash input directly.
		bf.AddPrehashed(sid8[:])
		propBloomBytes = bf.Bytes()
	}
	propagationPacked := packCGPRBBR(depth, propCkpt, propBloomBytes, propWitness)

	s.SetAttributes(
		attribute.Int(AttrBagPrio, priority),
		attribute.String(AttrBR, encodeBR(propagationPacked)),  // outgoing baggage
		attribute.String(AttrBREmit, encodeBR(emitPayload)),    // exported by checkpoints and leaves
		attribute.String(AttrD, encodeBR(varintEncode(depth))), // exported by interior non-checkpoints
		attribute.Int("depth", depth),
	)
}

// OnEnd implements [sdktrace.SpanProcessor].
func (p *CallGraphBridgeProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	// The wire entry is taken (and freed) here. One Attributes call serves classification and
	// encoding.
	attrs := s.Attributes()
	w := bridgeWires.take(s.SpanContext().SpanID())
	if (w != nil) != p.checkpointRange.enabled() {
		brokenRule("a bridge span has a wire entry exactly when the window is ranged (stored at OnStart)")
	}
	p.routeToPipeline(s, attrs, w, pathCheckpoint(s.SpanKind(), attrs, w))
}

// routeToPipeline counts the span, encodes it and buffers it by priority.
func (p *CallGraphBridgeProcessor) routeToPipeline(s sdktrace.ReadOnlySpan, attrs []attribute.KeyValue, w *bridgeWire, highPriority bool) {
	atomic.AddInt64(&p.spansReceived, 1)
	if highPriority {
		atomic.AddInt64(&p.cpReceived, 1)
	} else {
		atomic.AddInt64(&p.lpReceived, 1)
	}
	p.resourceOnce.Do(func() {
		p.resource = resourceField(s.Resource())
	})
	// `_br`, `_o` and `_d` come from the wire entry; policyCallGraph drops the internal attributes.
	entry := encodeSpan(s, attrs, w, highPriority, policyCallGraph)
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

// getConfigDiscoveryEndpoint is the address of the collector's config discovery endpoint.
func (p *CallGraphBridgeProcessor) getConfigDiscoveryEndpoint() string {
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
func (p *CallGraphBridgeProcessor) parseCheckpointDistance(config map[string]interface{}) int64 {
	const defaultCPD = int64(1) // every span is a checkpoint

	if config == nil {
		return defaultCPD
	}

	if cpdVal, exists := config["cpd"]; exists {
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
func (p *CallGraphBridgeProcessor) fetchFullConfig(ctx context.Context) error {
	config, err := discoverConfig(ctx, p.httpClient, p.getConfigDiscoveryEndpoint())
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
		cpd = 0 // range mode has no fixed distance or shared Bloom geometry
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

	if !rangeConfig.enabled() {
		// Fixed-distance mode: size the shared Bloom filter for the distance.
		bm, bk := bloom.EstimateParameters(uint(pbBloomCapacity(int(cpd))), DefaultBloomFPRate)
		BloomFilterM = bm
		BloomFilterK = bk
		backend.SetRTBloomParams(bm, bk)
	}

	slog.Info("Successfully discovered full config",
		"config_keys", len(config),
		"checkpoint_distance", cpd,
		"cpd_min", rangeConfig.min, "cpd_max", rangeConfig.max,
		"reverse_policy", policy.mode, "reverse_probability", policy.probability,
		"reverse_passthrough", policy.passthrough)
	return nil
}

// getConfigMap returns the config map.
func (p *CallGraphBridgeProcessor) getConfigMap() map[string]interface{} {
	p.configLock.RLock()
	defer p.configLock.RUnlock()

	result := make(map[string]interface{})
	for k, v := range p.configMap {
		result[k] = v
	}
	return result
}

// Shutdown implements [sdktrace.SpanProcessor]. It flushes the buffers and waits for every export.
func (p *CallGraphBridgeProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Info("🔴 CallGraphBridgeProcessor shutting down")

	// processEvents flushes one last time; then wait for the exports.
	close(p.stopChan)
	p.wg.Wait()
	p.exportWG.Wait()
	checkDrainedCounts(atomic.LoadInt64(&p.spansSent), atomic.LoadInt64(&p.spansDropped), atomic.LoadInt64(&p.spansFlushed))

	if err := p.conn.close(); err != nil {
		slog.Error("❌ Failed to stop client", "error", err)
	}

	slog.Info("✅ CallGraphBridgeProcessor shutdown complete",
		"spans_sent", atomic.LoadInt64(&p.spansSent),
		"spans_dropped", atomic.LoadInt64(&p.spansDropped))
	return nil
}

// ForceFlush implements [sdktrace.SpanProcessor]. It does nothing: spans are exported every
// SBExportInterval.
func (p *CallGraphBridgeProcessor) ForceFlush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return nil
}
