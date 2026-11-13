package otelcol

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"unsafe"
)

// encodeBR turns a packed `_br` payload into text for places that need valid UTF-8, such as OTLP
// string attributes. It uses unpadded URL-safe base64, which propagators do not re-encode.
func encodeBR(packed []byte) string {
	return base64.RawURLEncoding.EncodeToString(packed)
}

// decodeBR reverses encodeBR. It returns false for empty or malformed input.
func decodeBR(s string) ([]byte, bool) {
	if s == "" {
		return nil, false
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	return b, true
}

// propBR stores a packed `_br` in baggage as raw bytes in a Go string, with no base64 step. Carriers
// that need text encode it themselves (the JSON carrier sends non-UTF-8 values as base64). The
// legacy fixed-distance PB and CGPB still use the base64 form.
func propBR(packed []byte) string {
	return string(packed)
}

// viewPropBR is parsePropBR without the copy. The result aliases s and must not be written.
func viewPropBR(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// propBROwned is propBR without the copy. The caller hands over packed and must not write it again.
func propBROwned(packed []byte) string {
	return unsafe.String(unsafe.SliceData(packed), len(packed))
}

// parsePropBR reverses propBR. It returns a private copy, since callers may build a Bloom filter
// over the bytes and modify it. It returns false for "".
func parsePropBR(s string) ([]byte, bool) {
	if s == "" {
		return nil, false
	}
	return []byte(s), true
}

// decodeBRDepth reads only the leading depth varint of a base64 `_br`.
func decodeBRDepth(s string) (int, bool) {
	raw, ok := decodeBR(s)
	if !ok {
		return 0, false
	}
	depth, n := binary.Uvarint(raw)
	if n <= 0 {
		return 0, false
	}
	return int(depth), true
}

// Wire-format constants, identical to the trace simulator's so its size measurements carry over.
const (
	BRPropertyNameOverheadBytes = 3 // len("_br")

	// Bridge type IDs. The simulator also counts them as payload bytes.
	PBBridgeTypeID  = 1
	CGPBridgeTypeID = 2
	SBridgeTypeID   = 3

	DefaultBloomFPRate = 0.0001 // PB and CGPB Bloom filters

	BaggageKeyBytes = 3 // len("_br")
)

// BaggageBRKey is the baggage key that carries the packed bridge state between services.
const BaggageBRKey = "_br"

// AttrBR is the span attribute that becomes the outgoing `_br` baggage (legacy fixed-distance PB and
// CGPB). The instrumentation turns every `__bag.` attribute into baggage with the prefix stripped.
// The value is the state to propagate, so a checkpoint's is already reset.
const AttrBR = "__bag._br"

// AttrBREmit is the exported attribute of checkpoints (and leaves); its presence marks a span as
// high priority. It holds the state the span inherited, before any reset. On the wire it is one
// bytes value, uvarint(len(rc)) || rc || payload, where rc is the packed reverse context the span
// took over (a single 0x00 when there is none); see appendBridgeWireBytes and splitBR. In the
// legacy fixed-distance mode the in-process attribute holds the base64 payload alone.
const AttrBREmit = "_br"

// AttrOC is an older SB breadcrumb for non-checkpoints: the nearest checkpoint's ID and the
// ordinal chain from it, without end events. It let a surviving span place itself when its
// ancestors were shed. AttrO replaces it.
const AttrOC = "_oc"

// AttrO is the breadcrumb of an SB non-checkpoint: uvarint(child ordinal) || uvarint(depth), as a
// bytes value. With its parent span ID that is enough to place a surviving span; the chain between
// checkpoints lives only in the checkpoints' `_br`.
const AttrO = "_o"

// AttrDepth is an experimental breadcrumb: the depth alone, as an int64 attribute.
const AttrDepth = "d"

// OCNameDelim separates a span name from a breadcrumb appended to it ("<name>~<base64>"). It is not
// a base64url character, so a reader can split at the last one.
const OCNameDelim = "~"

// AttrBagPrio carries OnStart's priority decision to OnEnd in processors that keep it on the span.
// Like every `__bag.` attribute it is not exported; the collector decides priority from AttrBREmit.
const AttrBagPrio = "__bag.prio"

// AttrForceLP (true) makes SB classify a span as low priority whatever its depth. Load generators use
// it for synthetic spans: siblings started from one context would otherwise all be classified like
// their parent. Not exported.
const AttrForceLP = "__bag.force_lp"

// varintEncode encodes a non-negative integer as a protobuf-style varint.
func varintEncode(n int) []byte {
	if n < 0 {
		n = 0
	}
	return binary.AppendUvarint(nil, uint64(n))
}

// varintLen returns the byte length of varintEncode(n) without allocating.
func varintLen(n int) int {
	if n < 0 {
		n = 0
	}
	switch {
	case n < 1<<7:
		return 1
	case n < 1<<14:
		return 2
	case n < 1<<21:
		return 3
	case n < 1<<28:
		return 4
	case n < 1<<35:
		return 5
	case n < 1<<42:
		return 6
	case n < 1<<49:
		return 7
	case n < 1<<56:
		return 8
	}
	return 9
}

// packBR packs the PB baggage payload: varint(depthMod) || bloomBytes.
func packBR(depthMod int, bloomBytes []byte) []byte {
	out := make([]byte, 0, varintLen(depthMod)+len(bloomBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depthMod, 0)))
	out = append(out, bloomBytes...)
	return out
}

// unpackBR reverses packBR. bloomBytes aliases buf.
func unpackBR(buf []byte) (depthMod int, bloomBytes []byte, ok bool) {
	v, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, nil, false
	}
	return int(v), buf[n:], true
}

// AttrD is the PB/CGPB breadcrumb of an interior non-checkpoint: varint(absolute depth) as a bytes
// value. With its parent span ID that places a surviving span; the full `_br` is only on
// checkpoints and leaves.
const AttrD = "_d"

// pbBloomCapacity is the number of entries a PB Bloom filter is sized for: cpd-1. Only spans strictly
// between two checkpoints are ever tested (the anchor is named explicitly), and a payload holds
// the filter as inherited, before its own span is added. Reconstruction must size from the same
// capacity.
func pbBloomCapacity(cpd int) int {
	if cpd <= 2 {
		return 1
	}
	return cpd - 1
}

// packPathBridgeBR packs the PB payload in fixed-distance mode:
//
//	varint(absolute depth) || ckpt(8) || bloomBytes
//
// The exported `_br` holds the previous checkpoint's span ID and the inherited filter; the
// propagated one holds the span's current checkpoint (itself, if it is one) and the filter it
// passes on. An all-zero ckpt means the root.
func packPathBridgeBR(depth int, ckpt [8]byte, bloomBytes []byte) []byte {
	out := make([]byte, 0, varintLen(depth)+8+len(bloomBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt[:]...)
	out = append(out, bloomBytes...)
	return out
}

// unpackPathBridgeBR reverses packPathBridgeBR. bloomBytes aliases buf.
func unpackPathBridgeBR(buf []byte) (depth int, ckpt [8]byte, bloomBytes []byte, ok bool) {
	v, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, ckpt, nil, false
	}
	rest := buf[n:]
	if len(rest) < 8 {
		return 0, ckpt, nil, false
	}
	copy(ckpt[:], rest[:8])
	return int(v), ckpt, rest[8:], true
}

// packCGPRBBR packs the CGPB payload in fixed-distance mode: the PB payload plus the window's hash
// array (HA).
//
//	varint(absolute depth) || ckpt(8) || bloomBytes || haBytes
//
// The filter has a fixed width, so the HA is whatever follows it. A checkpoint propagates an empty
// HA. Entries are added with haAppendEntry.
func packCGPRBBR(depth int, ckpt [8]byte, bloomBytes, haBytes []byte) []byte {
	out := make([]byte, 0, varintLen(depth)+8+len(bloomBytes)+len(haBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt[:]...)
	out = append(out, bloomBytes...)
	out = append(out, haBytes...)
	return out
}

// unpackCGPRBBR reverses packCGPRBBR; bloomLen is the filter's width in bytes. The results alias buf.
func unpackCGPRBBR(buf []byte, bloomLen int) (depth int, ckpt [8]byte, bloomBytes, haBytes []byte, ok bool) {
	v, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, ckpt, nil, nil, false
	}
	rest := buf[n:]
	if len(rest) < 8+bloomLen {
		return 0, ckpt, nil, nil, false
	}
	copy(ckpt[:], rest[:8])
	rest = rest[8:]
	return int(v), ckpt, rest[:bloomLen], rest[bloomLen:], true
}

// packCGPBBR packs the CGPB baggage payload:
//
//	varint(depthMod) || bloomBytes || haBytes
func packCGPBBR(depthMod int, bloomBytes, haBytes []byte) []byte {
	out := make([]byte, 0, varintLen(depthMod)+len(bloomBytes)+len(haBytes))
	out = binary.AppendUvarint(out, uint64(maxInt(depthMod, 0)))
	out = append(out, bloomBytes...)
	out = append(out, haBytes...)
	return out
}

// unpackCGPBBR reverses packCGPBBR; bloomLen is the filter's width in bytes.
func unpackCGPBBR(buf []byte, bloomLen int) (depthMod int, bloomBytes, haBytes []byte, ok bool) {
	v, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, nil, nil, false
	}
	rest := buf[n:]
	if len(rest) < bloomLen {
		return 0, nil, nil, false
	}
	return int(v), rest[:bloomLen], rest[bloomLen:], true
}

// haAppendEntry appends a CGPB hash-array entry, parent span ID (8) || varint(depth), to a copy of
// ha. parentSpanID is hex; ha is returned unchanged if it does not parse.
func haAppendEntry(ha []byte, parentSpanID string, depthMod int) []byte {
	pid, ok := spanIDHexTo8Bytes(parentSpanID)
	if !ok {
		return ha
	}
	out := make([]byte, 0, len(ha)+8+varintLen(depthMod))
	out = append(out, ha...)
	out = append(out, pid[:]...)
	out = binary.AppendUvarint(out, uint64(maxInt(depthMod, 0)))
	return out
}

// spanIDHexTo8Bytes parses a hex span ID. Short input is zero-padded on the left; long input keeps
// its last 8 bytes.
func spanIDHexTo8Bytes(s string) ([8]byte, bool) {
	var out [8]byte
	if s == "" {
		return out, false
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return out, false
	}
	switch {
	case len(raw) == 8:
		copy(out[:], raw)
	case len(raw) > 8:
		copy(out[:], raw[len(raw)-8:])
	default:
		copy(out[8-len(raw):], raw)
	}
	return out, true
}

// traceIDHexTo16Bytes parses a hex trace ID like spanIDHexTo8Bytes; invalid input gives zeros.
func traceIDHexTo16Bytes(s string) [16]byte {
	var out [16]byte
	if s == "" {
		return out
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return out
	}
	switch {
	case len(raw) == 16:
		copy(out[:], raw)
	case len(raw) > 16:
		copy(out[:], raw[len(raw)-16:])
	default:
		copy(out[16-len(raw):], raw)
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
