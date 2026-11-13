package otelcol

// The single-buffer window encoder must produce exactly the bytes of the filter-object path.

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/blueprint-uservices/blueprint/runtime/plugins/bloom"
	"go.opentelemetry.io/otel/trace"
)

func referenceWindowPayloads(r checkpointRange, depth int, anchor [8]byte, distance int, inherited, witness []byte,
	isCheckpoint bool, outgoingTTL byte, sid trace.SpanID) (emit, wrapped []byte) {
	geometry := checkpointBlooms[distance-1]
	filter := bloom.NewFromBytes(inherited, geometry.m, geometry.k)
	emit = packCheckpointWindowBR(depth, anchor, distance, filter.Bytes(), witness)
	if isCheckpoint {
		anchor = [8]byte(sid)
		distance = int(outgoingTTL) + 1
		geometry = checkpointBlooms[distance-1]
		filter = bloom.New(geometry.m, geometry.k)
		witness = nil
	} else {
		filter.AddPrehashed(sid[:])
	}
	return emit, r.wrap(outgoingTTL, packCheckpointWindowBR(depth, anchor, distance, filter.Bytes(), witness))
}

func TestEncodeWindowPayloadsMatchesFilterPath(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 20000; i++ {
		r := checkpointRange{2, 6}
		if i%5 == 0 {
			r = checkpointRange{}
		}
		distance := 1 + rng.Intn(256)
		g := checkpointBlooms[distance-1]
		var inherited []byte
		switch rng.Intn(4) {
		case 0: // none (root / invalid parent)
		case 1: // wrong size: starts empty
			inherited = make([]byte, rng.Intn(g.bytes+3))
			rng.Read(inherited)
		default:
			inherited = make([]byte, g.bytes)
			rng.Read(inherited)
		}
		var anchor [8]byte
		var sid trace.SpanID
		rng.Read(anchor[:])
		rng.Read(sid[:])
		depth := rng.Intn(1 << uint(rng.Intn(20)))
		witness := randomWitness(rng, depth)
		isCheckpoint := rng.Intn(2) == 0
		ttl := byte(rng.Intn(256))
		if inherited != nil && len(inherited) != g.bytes {
			// A parent filter of the wrong width cannot come out of unpackCheckpointWindowBR, so the
			// encoder treats one as a broken invariant.
			mustBreakInvariant(t, "wrong-width inherited Bloom", func() {
				encodeWindowPayloads(r, depth, anchor, distance, inherited, witness, isCheckpoint, ttl, sid)
			})
			continue
		}
		we, ww := referenceWindowPayloads(r, depth, anchor, distance, inherited, witness, isCheckpoint, ttl, sid)
		ge, gw := encodeWindowPayloads(r, depth, anchor, distance, inherited, witness, isCheckpoint, ttl, sid)
		if !bytes.Equal(we, ge) || !bytes.Equal(ww, gw) {
			t.Fatalf("case %d: emit %v wrapped %v differ", i, bytes.Equal(we, ge), bytes.Equal(ww, gw))
		}
	}
}
