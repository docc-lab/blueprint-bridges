package otelcol

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// spanPadding is an exported `padding` attribute of SPAN_PADDING_BYTES bytes that the vanilla and SB
// processors add to every span, to inflate span size in experiments. Empty when unset.
var spanPadding = func() string {
	n, _ := strconv.Atoi(os.Getenv("SPAN_PADDING_BYTES"))
	if n <= 0 {
		return ""
	}
	return strings.Repeat("x", n)
}()

// grpcDeadline is the timeout of each export RPC: OTLP_DEADLINE_MS, default 1 s.
var grpcDeadline = func() time.Duration {
	ms, _ := strconv.Atoi(os.Getenv("OTLP_DEADLINE_MS"))
	if ms <= 0 {
		return 1 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}()

// grpcRetryEnabled reflects OTLP_RETRY (anything but "off"). The OTLP exporter client's retry does
// not exist on the direct export path; see sdk_retry.go for the retry the bridges use.
var grpcRetryEnabled = os.Getenv("OTLP_RETRY") != "off"

// Export tuning. ExportInterval bounds the delay between a span ending and its export; BatchSize
// keeps each export under the receiver's 4 MiB gRPC message limit.
const (
	ExportInterval = 100 * time.Millisecond
	BatchSize      = 512
	// MetricsInterval is how often the processors log their counters.
	MetricsInterval = 1 * time.Second
)

// VanillaProcessor exports every span, with no bridge state. It is the baseline the bridges are
// measured against and shares their export path.
type VanillaProcessor struct {
	mu sync.RWMutex

	conn          *otlpConn
	agentEndpoint string

	stopChan chan struct{}
	wg       sync.WaitGroup

	// exportWG tracks in-flight exports, so Shutdown can wait for them before closing the connection.
	exportWG sync.WaitGroup

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

	// resource is the encoded Resource. A TracerProvider has one, so it is encoded on the first OnEnd
	// and each batch carries it once.
	resource     []byte
	resourceOnce sync.Once

	// spanBuf holds ended spans, already encoded, until the next flush.
	spanBuf  []wireSpan
	spanLock sync.Mutex
}

// NewVanillaProcessor creates a processor that exports to the collector agent at agentEndpoint.
func NewVanillaProcessor(ctx context.Context, agentEndpoint string, additionalPort string) (*VanillaProcessor, error) {
	slog.Info("🔵 Creating new VanillaProcessor", "agentEndpoint", agentEndpoint)

	// A plain gRPC connection: the processor encodes the OTLP request itself (otlp_wire.go).
	conn, err := dialOTLP(agentEndpoint)
	if err != nil {
		slog.Error("❌ Failed to open the OTLP connection", "error", err)
		return nil, fmt.Errorf("failed to open the OTLP connection: %w", err)
	}
	if grpcRetryEnabled {
		slog.Warn("OTLP_RETRY is not off; the direct OTLP export path does not retry refused batches")
	}

	slog.Info("✅ VanillaProcessor created successfully")

	processor := &VanillaProcessor{
		conn:          conn,
		agentEndpoint: agentEndpoint,
		stopChan:      make(chan struct{}),
	}

	processor.wg.Add(1)
	go processor.processEvents()
	processor.wg.Add(1)
	go processor.logMetricsLoop()

	return processor, nil
}

// logMetricsLoop logs the counters every MetricsInterval until stopChan is closed.
func (p *VanillaProcessor) logMetricsLoop() {
	defer p.wg.Done()
	t := time.NewTicker(MetricsInterval)
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

// logMetrics logs every counter and the number of buffered spans.
func (p *VanillaProcessor) logMetrics() {
	p.spanLock.Lock()
	bufDepth := len(p.spanBuf)
	p.spanLock.Unlock()
	// Loaded later-incremented first, for the order checks (checkExportCounts).
	sent, dropped := atomic.LoadInt64(&p.spansSent), atomic.LoadInt64(&p.spansDropped)
	flushed := atomic.LoadInt64(&p.spansFlushed)
	received := atomic.LoadInt64(&p.spansReceived)
	checkExportCounts(sent, dropped, flushed, received)
	slog.Info("vanilla_processor_metrics",
		"spans_received", received,
		"spans_flushed", flushed,
		"spans_sent", sent,
		"batches_sent", atomic.LoadInt64(&p.batchesSent),
		"spans_dropped", dropped,
		"batches_dropped", atomic.LoadInt64(&p.batchesDropped),
		"send_deadline", atomic.LoadInt64(&p.sendDeadline),
		"send_unavailable", atomic.LoadInt64(&p.sendUnavailable),
		"send_exhausted", atomic.LoadInt64(&p.sendExhausted),
		"send_canceled", atomic.LoadInt64(&p.sendCanceled),
		"send_other", atomic.LoadInt64(&p.sendOther),
		"buffer_depth", bufDepth,
	)
	// The retry gauges go on a line of their own.
	sdkRetry.logMetrics()
}

// categorizeSendError counts a failed export under its gRPC status code.
func (p *VanillaProcessor) categorizeSendError(err error) {
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

// processEvents flushes the buffer every ExportInterval, and once more when stopped.
func (p *VanillaProcessor) processEvents() {
	defer p.wg.Done()

	ticker := time.NewTicker(ExportInterval)
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

// flushBuffer takes the buffered spans and exports them in batches of at most BatchSize, each in its
// own goroutine. A batch's request is the spans' encoded bytes concatenated under one Resource and
// one scope per instrumentation scope (otlp_wire.go).
func (p *VanillaProcessor) flushBuffer() {
	p.spanLock.Lock()
	snap := p.spanBuf
	p.spanBuf = make([]wireSpan, 0, cap(p.spanBuf))
	p.spanLock.Unlock()

	if len(snap) == 0 {
		return
	}

	batches(p.resource, snap, BatchSize, func(batch *wireBatch) {
		// Counted as flushed now; the export outcome adds it to sent or dropped.
		atomic.AddInt64(&p.spansFlushed, batch.n)
		p.exportWG.Add(1)
		go func(batch *wireBatch) {
			defer p.exportWG.Done()
			dropped := func(err error) {
				slog.Error("Failed to send batch", "error", err, "count", batch.n)
				atomic.AddInt64(&p.batchesDropped, 1)
				atomic.AddInt64(&p.spansDropped, batch.n)
				p.categorizeSendError(err)
				recordRefusedIDs(batch.traceIDs(), true) // every vanilla span is needed for its trace
			}
			sent := func() {
				slog.Debug("Sent batch", "count", batch.n)
				atomic.AddInt64(&p.batchesSent, 1)
				atomic.AddInt64(&p.spansSent, batch.n)
			}
			err := p.sendData(batch.req)
			// With BRIDGES_RETRY=all a refused batch is retried once (sdk_retry.go).
			if err != nil && sdkRetry.wants(true, err) {
				p.exportWG.Add(1)
				sdkRetry.scheduleBytes(int64(len(batch.req)), batch.n, func() error { return p.sendData(batch.req) }, func(err error) {
					defer p.exportWG.Done()
					defer batch.release()
					if err != nil {
						dropped(err)
						return
					}
					sent()
				})
				return
			}
			defer batch.release()
			if err != nil {
				dropped(err)
				return
			}
			sent()
		}(batch)
	})
}

// sendData sends one encoded batch to the OTLP endpoint.
func (p *VanillaProcessor) sendData(req []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), grpcDeadline)
	defer cancel()

	if err := p.conn.export(ctx, req); err != nil {
		slog.Error("❌ Failed to send data", "error", err)
		return fmt.Errorf("failed to send data: %w", err)
	}
	return nil
}

// startAttributeBaggage reports whether OnStart adds "__bag." attributes (see NewOTCollectorTracer).
func (p *VanillaProcessor) startAttributeBaggage() bool { return false }

// OnStart implements [sdktrace.SpanProcessor].
func (p *VanillaProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	if spanPadding != "" {
		s.SetAttributes(attribute.String("padding", spanPadding))
	}
}

// OnEnd implements [sdktrace.SpanProcessor].
func (p *VanillaProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	atomic.AddInt64(&p.spansReceived, 1)

	p.resourceOnce.Do(func() {
		p.resource = resourceField(s.Resource())
	})

	p.routeToPipeline(s)
}

// routeToPipeline encodes s and buffers it for the next flush.
func (p *VanillaProcessor) routeToPipeline(s sdktrace.ReadOnlySpan) {
	entry := encodeSpan(s, s.Attributes(), nil, false, policyVanilla)
	p.spanLock.Lock()
	p.spanBuf = append(p.spanBuf, entry)
	p.spanLock.Unlock()
}

// Shutdown implements [sdktrace.SpanProcessor]. It flushes the buffer and waits for every export.
func (p *VanillaProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Info("🔴 VanillaProcessor shutting down")

	close(p.stopChan) // processEvents flushes one last time
	p.wg.Wait()
	p.exportWG.Wait()
	checkDrainedCounts(atomic.LoadInt64(&p.spansSent), atomic.LoadInt64(&p.spansDropped), atomic.LoadInt64(&p.spansFlushed))

	if err := p.conn.close(); err != nil {
		slog.Error("❌ Failed to stop client", "error", err)
	}

	slog.Info("🔴 VanillaProcessor shutdown complete — final metrics:")
	p.logMetrics()
	return nil
}

// ForceFlush implements [sdktrace.SpanProcessor]. It does nothing: spans are exported every
// ExportInterval.
func (p *VanillaProcessor) ForceFlush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return nil
}
