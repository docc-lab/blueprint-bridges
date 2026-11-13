package backend

// The reverse context (retCtx): trusses that travel back up the call path in responses, so a span
// above can export them as a checkpoint when the span that produced them could not.
import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/plugins/bloom"
	"go.opentelemetry.io/otel/trace"
)

// Bloom geometry of legacy ancestry segments (SegAMQ). Checkpoint segments carry their own window
// distance in each truss instead, so windows of different sizes survive fan-in. The geometry comes
// from the bridge processor (SetRTBloomParams, sized for its checkpoint distance) or, without one,
// from the environment at startup:
//
//	RT_BLOOM_CAP  expected number of elements (default 8)
//	RT_BLOOM_FPR  target false-positive rate (default 0.0001)
var (
	rtBloomM atomic.Uint64
	rtBloomK atomic.Uint32
)

func init() {
	capacity := 8
	if n, err := strconv.Atoi(os.Getenv("RT_BLOOM_CAP")); err == nil && n > 0 {
		capacity = n
	}
	fpr := 0.0001
	if f, err := strconv.ParseFloat(os.Getenv("RT_BLOOM_FPR"), 64); err == nil && f > 0 {
		fpr = f
	}
	m, k := bloom.EstimateParameters(uint(capacity), fpr)
	rtBloomM.Store(m)
	rtBloomK.Store(uint32(k))
}

// SetRTBloomParams sets the legacy segment geometry to the bridge processor's (runtime/plugins/otelcol).
func SetRTBloomParams(m uint64, k uint) {
	if m > 0 && k > 0 {
		rtBloomM.Store(m)
		rtBloomK.Store(uint32(k))
	}
}

func rtBloom() (uint64, uint) { return rtBloomM.Load(), uint(rtBloomK.Load()) }

// TrussSegment is one piece of a reverse context.
type TrussSegment struct {
	Kind string `json:"k"` // amq | hash | ordinal | ee | dee | ...
	Data []byte `json:"d,omitempty"`
	// TTL counts down independently for each returned truss. A nil TTL leaves the decision to the
	// receiving SDK's probability policy, or, with the TTL policy, to the next scheduled checkpoint.
	ReverseTTL *byte `json:"ttl,omitempty"`
}

const (
	SegAMQ     = "amq"
	SegHash    = "hash"
	SegOrdinal = "ordinal"
	SegEndEv   = "ee"
	SegDelayEE = "dee"

	// Checkpoint segments preserve a complete SDK truss and its intended
	// location: spanID(8 bytes) || uvarint(absolute depth) || truss bytes.
	SegPathCheckpoint       = "checkpoint.pb"
	SegCallGraphCheckpoint  = "checkpoint.cgpb"
	SegStructuralCheckpoint = "checkpoint.sb"
	SegVanillaCheckpoint    = "checkpoint.v"
)

type trussData struct {
	FP   string         `json:"fp"`
	Par  string         `json:"parent,omitempty"`
	M    uint64         `json:"m,omitempty"`
	K    uint           `json:"k,omitempty"`
	Segs []TrussSegment `json:"segs,omitempty"`
}

func encodeTruss(t trussData) string {
	// Envelopes of checkpoint segments only are packed like the forward path. Anything with a legacy
	// ancestry segment or a parent fingerprint keeps the JSON envelope.
	if packableRetCtx(t) {
		return encodePackedRetCtx(t.Segs)
	}
	b, _ := json.Marshal(t)
	return base64.StdEncoding.EncodeToString(b)
}

func decodeTruss(s string) (trussData, bool) {
	var t trussData
	if s == "" {
		return t, false
	}
	// Packed envelopes are raw bytes starting with a version byte (as text, their RawURLEncoding:
	// RetCtxText); JSON envelopes are StdEncoding and start with '{'. They cannot be mistaken for each
	// other.
	if packed, ok := decodePackedRetCtx(s); ok {
		return packed, true
	}
	if raw := RetCtxFromText(s); raw != s {
		return decodePackedRetCtx(raw)
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return t, false
	}
	if json.Unmarshal(raw, &t) != nil {
		return t, false
	}
	return t, true
}

// ReturnedCheckpoint is a truss with the location it was meant to be checkpointed at. Forwarding and
// fan-in keep that origin, even when another span ends up exporting it. Truss is the SDK's `_br`
// bytes, before any reset.
type ReturnedCheckpoint struct {
	Kind       string
	SpanID     trace.SpanID
	Depth      uint64
	Truss      []byte
	ReverseTTL *byte
}

// EncodeCheckpointRetCtx encodes one checkpoint segment (span ID, depth, truss) as a reverse context.
func EncodeCheckpointRetCtx(spanID trace.SpanID, depth uint64, kind string, truss []byte) string {
	return encodeCheckpointRetCtx(spanID, depth, kind, truss, nil)
}

// EncodeCheckpointRetCtxWithTTL is EncodeCheckpointRetCtx with a reverse TTL, kept apart from the
// immutable spanID || depth || truss payload. Distance D starts at TTL D-1; each span upstream
// either absorbs the truss at TTL 0 or passes it on with the TTL decremented.
func EncodeCheckpointRetCtxWithTTL(spanID trace.SpanID, depth uint64, kind string, truss []byte, ttl byte) string {
	return encodeCheckpointRetCtx(spanID, depth, kind, truss, &ttl)
}

func encodeCheckpointRetCtx(spanID trace.SpanID, depth uint64, kind string, truss []byte, ttl *byte) string {
	// Callers are the span processors, encoding a truss for one of their own spans.
	if !spanID.IsValid() {
		InvariantBroken("a checkpoint truss names a valid span", "spanID=", spanID)
	}
	if _, ok := packedKindID[kind]; !ok {
		InvariantBroken("a checkpoint truss has a checkpoint kind", "kind=", kind)
	}
	data := append([]byte(nil), spanID[:]...)
	data = binary.AppendUvarint(data, depth)
	data = append(data, truss...)
	m, k := rtBloom()
	return encodeTruss(trussData{
		FP: spanID.String(), M: m, K: k,
		Segs: []TrussSegment{{Kind: kind, Data: data, ReverseTTL: ttl}},
	})
}

// DecodeReturnedCheckpoints decodes checkpoint locations and trusses. Legacy
// ancestry-only segments can coexist in the envelope and are skipped here.
func DecodeReturnedCheckpoints(retCtx string) ([]ReturnedCheckpoint, error) {
	t, ok := decodeTruss(retCtx)
	if !ok {
		return nil, fmt.Errorf("invalid reverse-context envelope")
	}
	var checkpoints []ReturnedCheckpoint
	for _, segment := range t.Segs {
		checkpoint, known, err := decodeCheckpointSegment(segment)
		if err != nil {
			return nil, err
		}
		if known {
			checkpoints = append(checkpoints, checkpoint)
		}
	}
	return checkpoints, nil
}

func decodeCheckpointSegment(segment TrussSegment) (ReturnedCheckpoint, bool, error) {
	switch segment.Kind {
	case SegPathCheckpoint, SegCallGraphCheckpoint, SegStructuralCheckpoint, SegVanillaCheckpoint:
	default:
		return ReturnedCheckpoint{}, false, nil
	}
	if len(segment.Data) < 9 {
		return ReturnedCheckpoint{}, true, fmt.Errorf("truncated %s checkpoint location", segment.Kind)
	}
	var spanID trace.SpanID
	copy(spanID[:], segment.Data[:8])
	depth, n := binary.Uvarint(segment.Data[8:])
	if !spanID.IsValid() || n <= 0 {
		return ReturnedCheckpoint{}, true, fmt.Errorf("invalid %s checkpoint location", segment.Kind)
	}
	return ReturnedCheckpoint{
		Kind: segment.Kind, SpanID: spanID, Depth: depth,
		Truss: segment.Data[8+n:], ReverseTTL: segment.ReverseTTL,
	}, true, nil
}

// RouteRetCtx splits a reverse context, at one span, into the part it checkpoints and the part it
// forwards. A scheduled checkpoint absorbs the whole bundle; any other span absorbs only the
// segments whose TTL ran out, so the others keep counting independently. Merging siblings costs no
// hop.
func RouteRetCtx(retCtx string, originalCheckpoint bool) (checkpointed, forwarded string) {
	return RouteRetCtxWithDecision(retCtx, originalCheckpoint, nil)
}

// RouteRetCtxWithDecision is RouteRetCtx with accept deciding on each valid truss that has no TTL.
// Segments with a TTL keep counting down. A nil accept gives RouteRetCtx; malformed or unknown
// segments without a TTL wait for a scheduled checkpoint to absorb the bundle.
func RouteRetCtxWithDecision(retCtx string, originalCheckpoint bool, accept func(ReturnedCheckpoint) bool) (checkpointed, forwarded string) {
	if retCtx == "" || originalCheckpoint {
		return retCtx, ""
	}
	t, ok := decodeTruss(retCtx)
	if !ok || len(t.Segs) == 0 {
		return "", retCtx // keep an opaque or legacy context for a checkpoint
	}
	var emitted, pending []TrussSegment
	changedTTL := false
	for _, segment := range t.Segs {
		if segment.ReverseTTL != nil && *segment.ReverseTTL == 0 {
			emitted = append(emitted, segment)
			continue
		}
		if segment.ReverseTTL != nil {
			ttl := *segment.ReverseTTL - 1
			segment.ReverseTTL = &ttl
			changedTTL = true
		} else if accept != nil {
			checkpoint, known, err := decodeCheckpointSegment(segment)
			if known && err == nil && accept(checkpoint) {
				emitted = append(emitted, segment)
				continue
			}
		}
		pending = append(pending, segment)
	}
	// Nothing absorbed: return the carrier unchanged rather than re-encode a possibly large bundle.
	if len(emitted) == 0 && !changedTTL {
		return "", retCtx
	}
	return encodeTrussSubset(t, emitted), encodeTrussSubset(t, pending)
}

func encodeTrussSubset(envelope trussData, segments []TrussSegment) string {
	if len(segments) == 0 {
		return ""
	}
	envelope.Segs = segments
	// A checkpoint-only partition packs; its fingerprints are derived from each segment's span ID.
	if packableRetCtx(envelope) {
		return encodePackedRetCtx(segments)
	}
	// Checkpoint origins stay with their partition. Legacy ancestry segments have envelope-level
	// fingerprints, so keep those if present.
	var fingerprints []string
	for _, segment := range segments {
		switch segment.Kind {
		case SegPathCheckpoint, SegCallGraphCheckpoint, SegStructuralCheckpoint, SegVanillaCheckpoint:
		default:
			return encodeTruss(envelope)
		}
		if len(segment.Data) < 9 {
			return encodeTruss(envelope)
		}
		var id trace.SpanID
		copy(id[:], segment.Data[:8])
		fingerprints = append(fingerprints, id.String())
	}
	envelope.FP = strings.Join(fingerprints, ",")
	return encodeTruss(envelope)
}

// DecodeRetCtx returns a reverse context's fingerprints, parent, Bloom geometry and ancestry segments,
// for the ancestry verifier and the sample logger.
func DecodeRetCtx(s string) (fp, parent string, m uint64, k uint, amqs [][]byte) {
	t, ok := decodeTruss(s)
	if !ok {
		return
	}
	for _, seg := range t.Segs {
		if seg.Kind == SegAMQ {
			amqs = append(amqs, seg.Data)
		}
	}
	return t.fingerprint(), t.Par, t.M, t.K, amqs
}

// BuildRetCtx creates a node's truss: an ancestry Bloom of its own and its parent's fingerprints as one
// segment, followed by the children's merged truss. It has no callers left.
func BuildRetCtx(ctx context.Context, traceCtx string, sc trace.SpanContext) string {
	m, k := rtBloom()
	bf := bloom.New(m, k)
	sid := sc.SpanID()
	bf.AddPrehashed(sid[:]) // this node's fingerprint
	parent := ""
	if traceCtx != "" {
		if cfg, _, err := GetSpanContext(traceCtx); err == nil {
			pid := cfg.SpanID
			bf.AddPrehashed(pid[:]) // parent's fingerprint (ancestor)
			if ParentIDEnabled() {
				parent = pid.String()
			}
		}
	}
	own := encodeTruss(trussData{
		FP:   sid.String(),
		Par:  parent,
		M:    m,
		K:    k,
		Segs: []TrussSegment{{Kind: SegAMQ, Data: bf.Bytes()}},
	})
	return MergeRetCtx(MergedChildren(ctx), own)
}

// MergeRetCtx concatenates two reverse contexts at fan-in: segment lists appended, fingerprints
// joined, geometry kept. Either may be empty.
func MergeRetCtx(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	// Two packed envelopes concatenate as bytes; fan-in must not decode and re-encode at every hop.
	if merged, ok := mergePackedRetCtx(a, b); ok {
		return merged
	}
	// Both come from this process's merges and checkpoint preparation, or from children, whose
	// reverse contexts the instrumentation checks on arrival; one that does not decode would lose
	// the trusses in it.
	ta, oka := decodeTruss(a)
	tb, okb := decodeTruss(b)
	if !oka {
		InvariantBroken("a reverse context being merged decodes", "len=", len(a))
	}
	if !okb {
		InvariantBroken("a reverse context being merged decodes", "len=", len(b))
	}
	m, k := ta.M, ta.K
	if m == 0 {
		m, k = tb.M, tb.K
	}
	fp := ta.fingerprint()
	if fpb := tb.fingerprint(); fpb != "" {
		if fp != "" {
			fp += ","
		}
		fp += fpb
	}
	par := ta.Par
	if par == "" {
		par = tb.Par
	}
	segs := append(append([]TrussSegment{}, ta.Segs...), tb.Segs...) // fresh slice, no aliasing
	return encodeTruss(trussData{FP: fp, Par: par, M: m, K: k, Segs: segs})
}

// The per-request fan-in accumulator.
type retMergeKey struct{}

// IsRetMergeKey says whether key is the context key under which AddToMerge and MergedChildren look
// up the accumulator (a *RetMerge).  For context types that answer several keys from one layer.
func IsRetMergeKey(key any) bool {
	_, ok := key.(retMergeKey)
	return ok
}

// RetMerge is the per-request fan-in accumulator. It is exported so a tracer can keep it in its own
// per-request context value (answering IsRetMergeKey) instead of adding a context layer with
// WithRetMerge. The zero value is ready to use.
type RetMerge struct {
	mu sync.Mutex
	s  string
}

func WithRetMerge(ctx context.Context) context.Context {
	return context.WithValue(ctx, retMergeKey{}, &RetMerge{})
}

func AddToMerge(ctx context.Context, child string) {
	if m, ok := ctx.Value(retMergeKey{}).(*RetMerge); ok {
		m.mu.Lock()
		m.s = MergeRetCtx(m.s, child)
		m.mu.Unlock()
	}
}

func MergedChildren(ctx context.Context) string {
	if m, ok := ctx.Value(retMergeKey{}).(*RetMerge); ok {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.s
	}
	return ""
}

// Settings from the environment.
func ReverseTrussEnabled() bool { return os.Getenv("REVERSE_TRUSS") == "on" }
func ParentIDEnabled() bool     { return os.Getenv("RT_PARENTID") == "on" }
func IsRoot() bool              { return os.Getenv("RT_ROOT") == "on" }

// ReverseTrussCheckpointKey (`_rc`) marks a span that exports a reverse context; the processors
// export it with checkpoint priority. Bridge spans do not use it: their reverse context is inside
// their `_br` attribute. Vanilla, which has no `_br`, exports `_rc` as text.
const ReverseTrussCheckpointKey = "_rc"

// Older captures may also contain a "bridges.forward_up" marker, which is no longer written.

// Counters, logged periodically and on SIGINT / SIGTERM.
var (
	rtCkpt, rtRecv, rtReject, rtLocal atomic.Uint64
	rtDumpOnce                        sync.Once
)

func installCounterDump() {
	rtDumpOnce.Do(func() {
		dump := func() {
			slog.Info("BRIDGES_RT",
				"checkpoints", rtCkpt.Load(),
				"received", rtRecv.Load(),
				"leaf_rejects", rtReject.Load(),
				"local_checkpoints", rtLocal.Load())
		}
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() { <-ch; dump(); os.Exit(0) }()
		sec := 15
		if n, err := strconv.Atoi(os.Getenv("RT_DUMP_SEC")); err == nil {
			sec = n
		}
		if sec > 0 {
			go func() {
				t := time.NewTicker(time.Duration(sec) * time.Second)
				for range t.C {
					dump()
				}
			}()
		}
	})
}

func CountCheckpoint()      { installCounterDump(); rtCkpt.Add(1) }
func CountTrussReceived()   { installCounterDump(); rtRecv.Add(1) }
func CountLeafReject()      { installCounterDump(); rtReject.Add(1) }
func CountLocalCheckpoint() { installCounterDump(); rtLocal.Add(1) }

var rtSample atomic.Uint64

// rtSampleN is the sampling period of SampleLogCheckpoint: RT_SAMPLE, read once.
var (
	rtSampleOnce   sync.Once
	rtSamplePeriod uint64
)

func rtSampleN() uint64 {
	rtSampleOnce.Do(func() {
		rtSamplePeriod = 500
		if n, err := strconv.Atoi(os.Getenv("RT_SAMPLE")); err == nil && n > 0 {
			rtSamplePeriod = uint64(n)
		}
	})
	return rtSamplePeriod
}

// SampleLogCheckpoint logs about one in RT_SAMPLE checkpoints with their origin, so the raw reverse
// context can be inspected with TestRTVerify.
func SampleLogCheckpoint(retCtx string) {
	if rtSample.Add(1)%rtSampleN() != 0 {
		return
	}
	fp, parent, m, k, amqs := DecodeRetCtx(retCtx)
	checkpoints, err := DecodeReturnedCheckpoints(retCtx)
	locations := make([]string, 0, len(checkpoints))
	for _, cp := range checkpoints {
		locations = append(locations, fmt.Sprintf("%s:%s@%d", cp.Kind, cp.SpanID, cp.Depth))
	}
	slog.Info("BRIDGES_CKPT", "fp", fp, "parent", parent, "m", m, "k", k,
		"amq_segments", len(amqs), "locations", locations, "decode_error", err, "retctx", RetCtxText(retCtx))
}
