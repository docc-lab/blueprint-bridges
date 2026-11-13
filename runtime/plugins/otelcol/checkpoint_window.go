package otelcol

import (
	"context"
	"encoding/binary"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/bloom"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// checkpointBloomGeometry is the Bloom filter size for one checkpoint distance.
type checkpointBloomGeometry struct {
	m     uint64
	k     uint
	bytes int
}

func (g checkpointBloomGeometry) emptyBytes() []byte { return make([]byte, g.bytes) }

// checkpointBlooms holds the geometry for every distance that fits in the one-byte distance field.
var checkpointBlooms = func() [256]checkpointBloomGeometry {
	var geometries [256]checkpointBloomGeometry
	for i := range geometries {
		m, k := bloom.EstimateParameters(uint(pbBloomCapacity(i+1)), DefaultBloomFPRate)
		geometries[i] = checkpointBloomGeometry{m, k, int((m + 7) / 8)}
	}
	return geometries
}()

// packCheckpointWindowBR encodes a PB/CGPB truss in range mode:
//
//	varint(absolute depth) || checkpoint span ID (8) || byte(distance - 1) || bloom || witness
//
// The witness is CGPB's fan-out witness, if the truss has one; PB has none. The distance fixes the
// Bloom filter's size, so a reader can find where the witness starts. Propagated baggage puts the TTL byte in front of all of this. A checkpoint exports the
// window it inherited and propagates the new one it opens; a root uses its first distance for both.
func packCheckpointWindowBR(depth int, ckpt [8]byte, distance int, bloomBytes, witness []byte) []byte {
	out := make([]byte, 0, varintLen(depth)+9+len(bloomBytes)+len(witness))
	out = binary.AppendUvarint(out, uint64(depth))
	out = append(out, ckpt[:]...)
	out = append(out, byte(distance-1))
	out = append(out, bloomBytes...)
	return append(out, witness...)
}

func unpackCheckpointWindowBR(raw []byte) (depth int, ckpt [8]byte, distance int, bloomBytes, witness []byte, ok bool) {
	d, n := binary.Uvarint(raw)
	if n <= 0 || d > uint64(^uint(0)>>1) || len(raw)-n < 9 {
		return
	}
	copy(ckpt[:], raw[n:n+8])
	distance = int(raw[n+8]) + 1
	geometry := checkpointBlooms[distance-1]
	rest := raw[n+9:]
	if len(rest) < geometry.bytes {
		return 0, ckpt, 0, nil, nil, false
	}
	return int(d), ckpt, distance, rest[:geometry.bytes], rest[geometry.bytes:], true
}

// onStartCheckpointWindow is OnStart for PB and CGPB in range mode. In fixed-distance mode the
// processors use their legacy format instead.
func onStartCheckpointWindow(parent context.Context, s sdktrace.ReadWriteSpan, r checkpointRange, callGraph bool) {
	var (
		hasParent     bool
		parentTTL     byte
		parentDepth   int
		anchor        [8]byte
		distance      int
		inherited     []byte
		parentWitness []byte
	)
	if bag := backend.GetBaggageFromContext(parent); bag != nil {
		if raw, valid := parsePropBR(bag[BaggageBRKey]); valid {
			parentTTL, raw = r.unwrap(raw)
			var ok bool
			parentDepth, anchor, distance, inherited, parentWitness, ok = unpackCheckpointWindowBR(raw)
			// The parent's `_br` was written by this bridge, so it must unpack, its TTL must be
			// below its distance, and it carries no fan-out witness (PB) or at most one (CGPB).
			if !ok {
				brokenRule("the parent's `_br` unpacks as a window")
			}
			if int(parentTTL) >= distance {
				brokenInt("a window's TTL is below its distance", "ttl", int(parentTTL), distance)
			}
			if !callGraph && len(parentWitness) != 0 {
				brokenInt("a PB window carries no fan-out witness", "witness bytes", len(parentWitness), 0)
			}
			if len(parentWitness) != 0 {
				checkParentWitness(parentWitness, parentDepth)
			}
			hasParent = true
		}
	}

	isCheckpoint, outgoingTTL := r.next(parentTTL, hasParent)
	depth := 0
	if hasParent {
		depth = parentDepth + 1
	} else {
		anchor = [8]byte{}
		distance = int(outgoingTTL) + 1
		inherited, parentWitness = nil, nil
	}
	// The fan-out witness: the second child gets a new one, the first inherits the parent's, later
	// children get none.
	var witness []byte
	if callGraph && hasParent {
		seq, isInt := parent.Value("seqNum").(int)
		if !isInt || seq < 1 {
			brokenInt("a CGPB span with a parent window has a child ordinal >= 1", "seqNum", seq, 1)
		}
		if seq == 1 {
			witness = append([]byte(nil), parentWitness...)
		} else if seq == 2 {
			witness = newFanoutWitness(trace.SpanFromContext(parent).SpanContext().SpanID(), depth)
		}
	}

	// Nothing goes on the OTel span itself: the instrumentation picks up the outgoing `_br` through
	// backend.AppendSpanBaggage, and the rest stays in the wire table for OnEnd and the response path.
	sid := s.SpanContext().SpanID()
	emit, wrapped := encodeWindowPayloads(r, depth, anchor, distance, inherited, witness, isCheckpoint, outgoingTTL, sid)
	bridgeWires.store(sid, &bridgeWire{emit: emit, depth: depth, priority: isCheckpoint, prop: propBR(wrapped)})
}

// encodeWindowPayloads builds a span's export payload (the window it inherited) and its outgoing
// `_br` (with the TTL in range mode). A checkpoint opens a new, empty window anchored on itself;
// any other span adds itself to the inherited window. Both go into one buffer, so the inherited
// Bloom bits are copied once. checkpoint_window_encode_test.go checks the result against
// packCheckpointWindowBR.
func encodeWindowPayloads(r checkpointRange, depth int, anchor [8]byte, distance int, inherited, witness []byte,
	isCheckpoint bool, outgoingTTL byte, sid trace.SpanID) (emit, wrapped []byte) {
	if distance < 1 || distance > len(checkpointBlooms) {
		brokenInt("a window's distance is 1..256", "distance", distance, len(checkpointBlooms))
	}
	geometry := checkpointBlooms[distance-1]
	if len(inherited) != geometry.bytes {
		// Only a root starts with an empty window; a parent's filter always has its distance's width.
		if inherited != nil {
			brokenInt("an inherited Bloom has its distance's width", "bytes", len(inherited), geometry.bytes)
		}
		inherited = nil
	}
	propAnchor, propDistance, propGeometry, propInherited, propWitness := anchor, distance, geometry, inherited, witness
	if isCheckpoint {
		propAnchor, propDistance, propWitness, propInherited = [8]byte(sid), int(outgoingTTL)+1, nil, nil
		propGeometry = checkpointBlooms[propDistance-1]
	}
	head := varintLen(depth) + 9
	emitLen := head + geometry.bytes + len(witness)
	ttlLen := 0
	if r.enabled() {
		ttlLen = 1
	}
	buf := make([]byte, 0, emitLen+ttlLen+head+propGeometry.bytes+len(propWitness))
	buf = appendWindowBR(buf, depth, anchor, distance, inherited, geometry.bytes, witness)
	emit = buf[:emitLen:emitLen]
	if ttlLen == 1 {
		buf = append(buf, outgoingTTL)
	}
	bloomAt := len(buf) + head
	buf = appendWindowBR(buf, depth, propAnchor, propDistance, propInherited, propGeometry.bytes, propWitness)
	if !isCheckpoint {
		bloom.SetPrehashed(buf[bloomAt:bloomAt+geometry.bytes], geometry.m, geometry.k, sid[:])
	}
	return emit, buf[emitLen:]
}

// appendWindowBR appends the range-mode truss with bloomLen Bloom bytes: the given bits, or
// zeros when bits is nil.
func appendWindowBR(dst []byte, depth int, ckpt [8]byte, distance int, bits []byte, bloomLen int, witness []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(depth))
	dst = append(dst, ckpt[:]...)
	dst = append(dst, byte(distance-1))
	if bits != nil {
		dst = append(dst, bits...)
	} else {
		dst = append(dst, make([]byte, bloomLen)...)
	}
	return append(dst, witness...)
}
