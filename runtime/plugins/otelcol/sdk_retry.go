package otelcol

// Optional one-shot retry of refused export batches, chosen by BRIDGES_RETRY:
//
//	off  (default) a refused batch is dropped and counted (refused_ids.go).
//	hp   bridges: a refused high-priority batch is retried once, after BRIDGES_RETRY_DELAY_MS plus
//	     up to 50 % jitter. While one waits, low-priority batches are dropped locally instead of
//	     sent: the collector would refuse them anyway.
//	all  vanilla's equivalent: every refused batch is retried once; nothing is suppressed.
//
// A batch is held only between its refusal and its retry, so memory is bounded by refusal rate
// times delay. The pending depth and its high-water marks are logged with the processor metrics.

import (
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	retryOff = "off"
	retryHP  = "hp"
	retryAll = "all"
)

type retryGate struct {
	mode  string
	delay time.Duration

	pendingBatches atomic.Int64
	pendingSpans   atomic.Int64
	pendingBytes   atomic.Int64

	maxSpans    atomic.Int64 // high-water since start
	maxBytes    atomic.Int64
	winMaxSpans atomic.Int64 // high-water since the last metrics line
	winMaxBytes atomic.Int64

	retriedSpans   atomic.Int64 // spans whose batch was retried
	retryOKSpans   atomic.Int64 // ... accepted on the retry
	retryFailSpans atomic.Int64 // ... refused again, then dropped
	lpSuppressed   atomic.Int64 // low-priority spans dropped while a retry waited
}

func newRetryGate(mode string, delayMS string) *retryGate {
	switch mode {
	case retryHP, retryAll:
	default:
		mode = retryOff
	}
	ms, err := strconv.Atoi(delayMS)
	if err != nil || ms <= 0 {
		ms = 300
	}
	return &retryGate{mode: mode, delay: time.Duration(ms) * time.Millisecond}
}

// errLPSuppressed marks an LP batch dropped locally while an HP batch awaited its retry.
var errLPSuppressed = status.Error(codes.ResourceExhausted, "sdk: LP batch suppressed while an HP batch awaits its retry")

// sdkRetry is process-wide; there is one span processor per service.
var sdkRetry = newRetryGate(os.Getenv("BRIDGES_RETRY"), os.Getenv("BRIDGES_RETRY_DELAY_MS"))

// retryable reports whether an error is worth a retry: a collector refusal (Unavailable, or
// ResourceExhausted from low-priority shedding) or an export that timed out under load.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	}
	return false
}

// wants reports whether a refused batch of this priority gets its one retry.
func (g *retryGate) wants(isHP bool, err error) bool {
	switch g.mode {
	case retryHP:
		return isHP && retryable(err)
	case retryAll:
		return retryable(err)
	}
	return false
}

// suppressLP reports whether an LP batch must be dropped locally instead of sent.
func (g *retryGate) suppressLP() bool {
	return g.mode == retryHP && g.pendingBatches.Load() > 0
}

func bumpMax(v *atomic.Int64, x int64) {
	for {
		cur := v.Load()
		if x <= cur || v.CompareAndSwap(cur, x) {
			return
		}
	}
}

// schedule holds a refused batch and retries it once after the delay. resend sends it; done is
// called exactly once with the result (nil if accepted).
func (g *retryGate) schedule(events []*tracepb.ResourceSpans, n int64, resend func() error, done func(error)) {
	var size int64
	for _, rs := range events {
		size += int64(proto.Size(rs))
	}
	g.scheduleBytes(size, n, resend, done)
}

// scheduleBytes is schedule for an encoded batch of size bytes.
func (g *retryGate) scheduleBytes(size, n int64, resend func() error, done func(error)) {
	if n < 1 || size < 1 { // a refused batch has spans and request bytes
		brokenOrder("a retried batch has spans and bytes", "spans", n, size)
	}
	g.pendingBatches.Add(1)
	spans := g.pendingSpans.Add(n)
	bytes := g.pendingBytes.Add(size)
	bumpMax(&g.maxSpans, spans)
	bumpMax(&g.maxBytes, bytes)
	bumpMax(&g.winMaxSpans, spans)
	bumpMax(&g.winMaxBytes, bytes)
	g.retriedSpans.Add(n)
	wait := g.delay + time.Duration(rand.Int63n(int64(g.delay)/2+1))
	go func() {
		time.Sleep(wait)
		err := resend()
		spans, bytes, batches := g.pendingSpans.Add(-n), g.pendingBytes.Add(-size), g.pendingBatches.Add(-1)
		if spans < 0 || bytes < 0 || batches < 0 { // each retry takes back exactly what it added
			brokenOrder("the pending retry counts stay non-negative", "batches", batches, 0)
		}
		if err == nil {
			g.retryOKSpans.Add(n)
		} else {
			g.retryFailSpans.Add(n)
		}
		done(err)
	}()
}

// logMetrics logs the retry gauges on a BRIDGES_RETRY_metrics line. It logs nothing when the policy
// is off.
func (g *retryGate) logMetrics() {
	if g.mode == retryOff {
		return
	}
	slog.Info("BRIDGES_RETRY_metrics", g.fields()...)
}

// fields returns the retry gauges and restarts the high-water window.
func (g *retryGate) fields() []any {
	// A retry's outcome is counted after the retry itself (see checkExportCounts for the order).
	ok, failed := g.retryOKSpans.Load(), g.retryFailSpans.Load()
	retried := g.retriedSpans.Load()
	if ok+failed > retried {
		brokenOrder("every accepted or refused retry was scheduled", "ok+failed", ok+failed, retried)
	}
	return []any{
		"retry_mode_" + g.mode, 1,
		"retry_delay_ms", g.delay.Milliseconds(),
		"retry_pending_spans", g.pendingSpans.Load(),
		"retry_pending_bytes", g.pendingBytes.Load(),
		"retry_window_max_spans", g.winMaxSpans.Swap(g.pendingSpans.Load()),
		"retry_window_max_bytes", g.winMaxBytes.Swap(g.pendingBytes.Load()),
		"retry_max_spans", g.maxSpans.Load(),
		"retry_max_bytes", g.maxBytes.Load(),
		"retried_spans", retried,
		"retry_ok_spans", ok,
		"retry_fail_spans", failed,
		"lp_suppressed_spans", g.lpSuppressed.Load(),
	}
}
