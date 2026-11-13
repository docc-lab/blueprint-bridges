package otelcol

import (
	"encoding/binary"
	"math/bits"
	"sort"
)

// SB payload layout. An SB truss is the CGPB window core followed by SB's own start ordinals and
// end events. One packed payload (before the TTL byte that checkpointRange.wrap adds to propagated
// baggage):
//
//	varint(absolute depth) || checkpoint(8) || byte(window distance - 1)
//	|| bloom[geometry(distance)]                       # window core: the ancestors' filter
//	|| varint(len HA) || HA                            # window core: fan-out entries
//	|| varint(n) || n * ( varint(ordinal) || endGroup(bound = ordinal-1) )
//	|| varint(m) || m * ( trace_id(16) || parent(8) || varint(k) || endGroup(bound = k) )
//
// The first four fields match the PB/CGPB range-mode core byte for byte, so
// unpackCheckpointWindowBR reads them unchanged. The HA is length-prefixed here (CGPB leaves it as
// the remainder) so the tail can follow it.
//
// Ordinals: one start ordinal per span in the window, in path order, ending with the span that
// carries the payload. Entry i's end group lists the siblings that had ended before span i
// started, as its parent saw them. Delayed entries are end events a parent saw after its last
// child started (minus the implied last one), stamped with the parent's trace and span ID and
// attached to the next outgoing call of that process.
//
// endGroup: varint(count<<1 | lehmer). With lehmer=1 the group is a bitmap of `bound` bits marking
// which ordinals ended, followed by their order as Lehmer digits packed into mixed-radix uvarint
// words. With lehmer=0 it is count plain varints (builds with -tags sb_nolehmer, or ends that are
// not a distinct subset of 1..bound).

type structuralDelayed struct {
	traceID  [16]byte
	parent   [8]byte
	children int
	ends     []int
}

type structuralTail struct {
	ordinals  []int
	endEvents [][]int // endEvents[i] belongs to ordinals[i]
	delayed   []structuralDelayed
}

// mixedRadixLimit bounds one packed word so the arithmetic stays in uint64.
const mixedRadixLimit = uint64(1) << 62

// lehmerDigits maps a permutation of distinct values to its Lehmer digits:
// digit i is the rank of perm[i] among the values not yet placed.
func lehmerDigits(perm []int) []int {
	// One allocation holds both the sorted working set and the digits.
	scratch := make([]int, 2*len(perm))
	remaining, digits := scratch[:len(perm)], scratch[len(perm):]
	copy(remaining, perm)
	sort.Ints(remaining)
	for i, value := range perm {
		index := sort.SearchInts(remaining, value)
		digits[i] = index
		remaining = append(remaining[:index], remaining[index+1:]...)
	}
	return digits
}

func lehmerPermutation(sorted []int, digits []int) ([]int, bool) {
	if len(digits) == 0 {
		return nil, true
	}
	remaining := append([]int(nil), sorted...)
	perm := make([]int, 0, len(digits))
	for _, digit := range digits {
		if digit < 0 || digit >= len(remaining) {
			return nil, false
		}
		perm = append(perm, remaining[digit])
		remaining = append(remaining[:digit], remaining[digit+1:]...)
	}
	return perm, true
}

// mixedRadixChunks splits digit positions of a permutation of n elements into
// runs whose radix product stays below mixedRadixLimit. Encoder and decoder
// derive identical boundaries from n alone.
func mixedRadixChunks(n int) [][2]int {
	var chunks [][2]int
	start := 0
	product := uint64(1)
	for i := 0; i < n; i++ {
		radix := uint64(n - i)
		if radix > mixedRadixLimit/product && i > start { // overflow-safe product bound
			chunks = append(chunks, [2]int{start, i})
			start, product = i, 1
		}
		product *= radix
	}
	if n > 0 {
		chunks = append(chunks, [2]int{start, n})
	}
	return chunks
}

// mixedRadixChunkEnd returns where the chunk starting at digit position start ends, and the chunk's
// radix product (every word of the chunk is below it). The boundaries are those of
// mixedRadixChunks, computed one at a time without allocating.
func mixedRadixChunkEnd(n, start int) (end int, product uint64) {
	product = 1
	for i := start; i < n; i++ {
		radix := uint64(n - i)
		if radix > mixedRadixLimit/product && i > start {
			return i, product
		}
		product *= radix
	}
	return n, product
}

func packLehmer(out []byte, digits []int) []byte {
	n := len(digits)
	for start, end := 0, 0; start < n; start = end {
		end, _ = mixedRadixChunkEnd(n, start)
		word := uint64(0)
		for i := start; i < end; i++ {
			word = word*uint64(n-i) + uint64(digits[i])
		}
		out = binary.AppendUvarint(out, word)
	}
	return out
}

// appendLehmerSmall packs the Lehmer code of perm (distinct values in 1..64) without allocating:
// digit i, the rank of perm[i] among the values not yet placed, is counted in a 64-bit set. Same
// bytes as packLehmer(out, lehmerDigits(perm)).
func appendLehmerSmall(out []byte, perm []int) []byte {
	var remaining uint64
	for _, v := range perm {
		remaining |= 1 << uint(v-1)
	}
	n, i := len(perm), 0
	for start, end := 0, 0; start < n; start = end {
		end, _ = mixedRadixChunkEnd(n, start)
		word := uint64(0)
		for ; i < end; i++ {
			bit := uint64(1) << uint(perm[i]-1)
			digit := bits.OnesCount64(remaining & (bit - 1))
			remaining &^= bit
			word = word*uint64(n-i) + uint64(digit)
		}
		out = binary.AppendUvarint(out, word)
	}
	return out
}

func unpackLehmer(buf []byte, n int) (digits []int, rest []byte, ok bool) {
	digits = make([]int, n)
	for start, end := 0, 0; start < n; start = end {
		end, _ = mixedRadixChunkEnd(n, start)
		word, read := binary.Uvarint(buf)
		if read <= 0 {
			return nil, nil, false
		}
		buf = buf[read:]
		for i := end - 1; i >= start; i-- {
			radix := uint64(n - i)
			digits[i] = int(word % radix)
			word /= radix
		}
		if word != 0 {
			return nil, nil, false
		}
	}
	return digits, buf, true
}

func lehmerApplicable(ends []int, bound int) bool {
	if bound <= 0 || len(ends) > bound {
		return false
	}
	var small uint64 // membership set without allocation, for bound <= 64
	var large []bool
	if bound > 64 {
		large = make([]bool, bound+1)
	}
	for _, e := range ends {
		if e < 1 || e > bound {
			return false
		}
		if large != nil {
			if large[e] {
				return false
			}
			large[e] = true
		} else {
			if small&(1<<uint(e-1)) != 0 {
				return false
			}
			small |= 1 << uint(e-1)
		}
	}
	return true
}

// packEndGroup appends one end-event group. bound is the largest ordinal the
// group may contain (own ordinal - 1 for inherited groups, child count for
// delayed groups).
func packEndGroup(out []byte, ends []int, bound int) []byte {
	if len(ends) == 0 {
		return append(out, 0) // empty group: one byte, no bitmap
	}
	if lehmerEnabled && lehmerApplicable(ends, bound) {
		out = binary.AppendUvarint(out, uint64(len(ends))<<1|1)
		// The bitmap is written in place (appending a make([]byte) does not allocate).
		at := len(out)
		out = append(out, make([]byte, (bound+7)/8)...)
		for _, e := range ends {
			out[at+(e-1)/8] |= 1 << uint((e-1)%8)
		}
		if bound <= 64 {
			return appendLehmerSmall(out, ends)
		}
		return packLehmer(out, lehmerDigits(ends))
	}
	out = binary.AppendUvarint(out, uint64(len(ends))<<1)
	for _, e := range ends {
		if e < 1 {
			// Ordinals start at 1. They can exceed bound: a later sibling can end before an earlier one starts.
			brokenInt("an end event is a child ordinal >= 1", "ordinal", e, 1)
		}
		out = binary.AppendUvarint(out, uint64(e))
	}
	return out
}

func unpackEndGroup(buf []byte, bound int) (ends []int, rest []byte, ok bool) {
	header, read := binary.Uvarint(buf)
	if read <= 0 {
		return nil, nil, false
	}
	buf = buf[read:]
	count := int(header >> 1)
	if count == 0 {
		return nil, buf, header&1 == 0
	}
	if header&1 == 0 {
		// Every value takes at least one byte, so a larger count is corrupt input from the network,
		// not a reason to allocate.
		if count > len(buf) {
			return nil, nil, false
		}
		ends = make([]int, 0, count)
		for i := 0; i < count; i++ {
			v, n := binary.Uvarint(buf)
			if n <= 0 {
				return nil, nil, false
			}
			buf = buf[n:]
			ends = append(ends, int(v))
		}
		return ends, buf, true
	}
	if bound <= 0 || count > bound {
		return nil, nil, false
	}
	width := (bound + 7) / 8
	if len(buf) < width {
		return nil, nil, false
	}
	sorted := make([]int, 0, count)
	for e := 1; e <= bound; e++ {
		if buf[(e-1)/8]&(1<<uint((e-1)%8)) != 0 {
			sorted = append(sorted, e)
		}
	}
	buf = buf[width:]
	if len(sorted) != count {
		return nil, nil, false
	}
	digits, buf, ok := unpackLehmer(buf, count)
	if !ok {
		return nil, nil, false
	}
	ends, ok = lehmerPermutation(sorted, digits)
	return ends, buf, ok
}

func packStructuralDelayed(out []byte, d structuralDelayed) []byte {
	out = append(out, d.traceID[:]...)
	out = append(out, d.parent[:]...)
	out = binary.AppendUvarint(out, uint64(maxInt(d.children, 0)))
	return packEndGroup(out, d.ends, d.children)
}

func unpackStructuralDelayed(buf []byte) (d structuralDelayed, rest []byte, ok bool) {
	if len(buf) < 24 {
		return d, nil, false
	}
	copy(d.traceID[:], buf[:16])
	copy(d.parent[:], buf[16:24])
	buf = buf[24:]
	k, read := binary.Uvarint(buf)
	if read <= 0 {
		return d, nil, false
	}
	d.children = int(k)
	d.ends, rest, ok = unpackEndGroup(buf[read:], d.children)
	return d, rest, ok
}

func packStructuralTail(out []byte, tail structuralTail) []byte {
	out = binary.AppendUvarint(out, uint64(len(tail.ordinals)))
	for i, ordinal := range tail.ordinals {
		out = binary.AppendUvarint(out, uint64(maxInt(ordinal, 0)))
		var ends []int
		if i < len(tail.endEvents) {
			ends = tail.endEvents[i]
		}
		out = packEndGroup(out, ends, ordinal-1)
	}
	out = binary.AppendUvarint(out, uint64(len(tail.delayed)))
	for _, d := range tail.delayed {
		out = packStructuralDelayed(out, d)
	}
	return out
}

func unpackStructuralTail(buf []byte) (tail structuralTail, ok bool) {
	n, read := binary.Uvarint(buf)
	if read <= 0 || n > uint64(len(buf)) {
		return tail, false
	}
	buf = buf[read:]
	tail.ordinals = make([]int, 0, n)
	tail.endEvents = make([][]int, 0, n)
	for i := uint64(0); i < n; i++ {
		ordinal, r := binary.Uvarint(buf)
		if r <= 0 {
			return tail, false
		}
		var ends []int
		ends, buf, ok = unpackEndGroup(buf[r:], int(ordinal)-1)
		if !ok {
			return tail, false
		}
		tail.ordinals = append(tail.ordinals, int(ordinal))
		tail.endEvents = append(tail.endEvents, ends)
	}
	m, read := binary.Uvarint(buf)
	if read <= 0 || m > uint64(len(buf)) {
		return tail, false
	}
	buf = buf[read:]
	for i := uint64(0); i < m; i++ {
		var d structuralDelayed
		d, buf, ok = unpackStructuralDelayed(buf)
		if !ok {
			return tail, false
		}
		tail.delayed = append(tail.delayed, d)
	}
	return tail, len(buf) == 0
}

// packStructuralBR packs the complete S-Bridge payload described above.
func packStructuralBR(depth int, ckpt [8]byte, distance int, bloomBytes, ha []byte, tail structuralTail) []byte {
	out := make([]byte, 0, varintLen(depth)+9+len(bloomBytes)+varintLen(len(ha))+len(ha)+8*(len(tail.ordinals)+1)+40*len(tail.delayed))
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt[:]...)
	out = append(out, byte(distance-1))
	out = append(out, bloomBytes...)
	out = binary.AppendUvarint(out, uint64(len(ha)))
	out = append(out, ha...)
	return packStructuralTail(out, tail)
}

// unpackStructuralBR reverses packStructuralBR. bloomBytes and ha are
// sub-slices of raw; the tail is copied out.
func unpackStructuralBR(raw []byte) (depth int, ckpt [8]byte, distance int, bloomBytes, ha []byte, tail structuralTail, ok bool) {
	d, n := binary.Uvarint(raw)
	if n <= 0 || d > uint64(^uint(0)>>1) || len(raw)-n < 9 {
		return
	}
	copy(ckpt[:], raw[n:n+8])
	distance = int(raw[n+8]) + 1
	geometry := checkpointBlooms[distance-1]
	rest := raw[n+9:]
	if len(rest) < geometry.bytes {
		return 0, ckpt, 0, nil, nil, tail, false
	}
	bloomBytes, rest = rest[:geometry.bytes], rest[geometry.bytes:]
	haLen, read := binary.Uvarint(rest)
	if read <= 0 || haLen > uint64(len(rest)-read) {
		return 0, ckpt, 0, nil, nil, tail, false
	}
	rest = rest[read:]
	ha, rest = rest[:haLen], rest[haLen:]
	tail, ok = unpackStructuralTail(rest)
	if !ok {
		return 0, ckpt, 0, nil, nil, structuralTail{}, false
	}
	return int(d), ckpt, distance, bloomBytes, ha, tail, true
}

// A child's tail is its parent's tail plus one entry, so the inherited part is copied as bytes
// instead of decoded and re-encoded on every span. The scan below validates exactly what
// unpackStructuralTail validates, without building anything.

// rawStructuralTail is a packed tail split into its two lists, as bytes.
type rawStructuralTail struct {
	n       uint64 // (ordinal, end group) entries
	entries []byte // their bytes
	m       uint64 // delayed entries
	delayed []byte // their bytes
}

// skipEndGroup returns what follows one end-event group, accepting exactly what unpackEndGroup
// accepts: for a Lehmer group, a bitmap with count bits set and words that decode completely
// (each below its chunk's radix product; their digits then always form a permutation).
func skipEndGroup(buf []byte, bound int) (rest []byte, ok bool) {
	header, read := binary.Uvarint(buf)
	if read <= 0 {
		return nil, false
	}
	buf = buf[read:]
	count := int(header >> 1)
	if count == 0 {
		return buf, header&1 == 0
	}
	if header&1 == 0 {
		for i := 0; i < count; i++ {
			_, n := binary.Uvarint(buf)
			if n <= 0 {
				return nil, false
			}
			buf = buf[n:]
		}
		return buf, true
	}
	if bound <= 0 || count > bound {
		return nil, false
	}
	width := (bound + 7) / 8
	if len(buf) < width {
		return nil, false
	}
	set := 0
	for i, b := range buf[:width] {
		if i == width-1 && bound%8 != 0 {
			b &= byte(1)<<uint(bound%8) - 1 // unpackEndGroup ignores bits above bound
		}
		set += bits.OnesCount8(b)
	}
	if set != count {
		return nil, false
	}
	buf = buf[width:]
	for start, end := 0, 0; start < count; start = end {
		var product uint64
		end, product = mixedRadixChunkEnd(count, start)
		word, n := binary.Uvarint(buf)
		if n <= 0 || word >= product {
			return nil, false
		}
		buf = buf[n:]
	}
	return buf, true
}

// scanStructuralTail splits a packed tail (the whole remainder of a payload) into its two lists.
func scanStructuralTail(buf []byte) (tail rawStructuralTail, ok bool) {
	n, read := binary.Uvarint(buf)
	if read <= 0 || n > uint64(len(buf)) {
		return tail, false
	}
	buf = buf[read:]
	start := buf
	for i := uint64(0); i < n; i++ {
		ordinal, r := binary.Uvarint(buf)
		if r <= 0 {
			return tail, false
		}
		if buf, ok = skipEndGroup(buf[r:], int(ordinal)-1); !ok {
			return tail, false
		}
	}
	tail.n, tail.entries = n, start[:len(start)-len(buf)]
	m, read := binary.Uvarint(buf)
	if read <= 0 || m > uint64(len(buf)) {
		return tail, false
	}
	buf = buf[read:]
	start = buf
	for i := uint64(0); i < m; i++ {
		if len(buf) < 24 {
			return tail, false
		}
		k, r := binary.Uvarint(buf[24:])
		if r <= 0 {
			return tail, false
		}
		if buf, ok = skipEndGroup(buf[24+r:], int(k)); !ok {
			return tail, false
		}
	}
	tail.m, tail.delayed = m, start[:len(start)-len(buf)]
	return tail, len(buf) == 0
}

// scanStructuralBR is unpackStructuralBR with the tail left packed (rawStructuralTail); bloomBytes,
// ha and the tail's bytes are sub-slices of raw.
func scanStructuralBR(raw []byte) (depth int, ckpt [8]byte, distance int, bloomBytes, ha []byte, tail rawStructuralTail, ok bool) {
	d, n := binary.Uvarint(raw)
	if n <= 0 || d > uint64(^uint(0)>>1) || len(raw)-n < 9 {
		return
	}
	copy(ckpt[:], raw[n:n+8])
	distance = int(raw[n+8]) + 1
	geometry := checkpointBlooms[distance-1]
	rest := raw[n+9:]
	if len(rest) < geometry.bytes {
		return 0, ckpt, 0, nil, nil, tail, false
	}
	bloomBytes, rest = rest[:geometry.bytes], rest[geometry.bytes:]
	haLen, read := binary.Uvarint(rest)
	if read <= 0 || haLen > uint64(len(rest)-read) {
		return 0, ckpt, 0, nil, nil, tail, false
	}
	rest = rest[read:]
	ha, rest = rest[:haLen], rest[haLen:]
	if tail, ok = scanStructuralTail(rest); !ok {
		return 0, ckpt, 0, nil, nil, rawStructuralTail{}, false
	}
	return int(d), ckpt, distance, bloomBytes, ha, tail, true
}

// appendStructuralHeader writes the payload's window core: what packStructuralBR writes before the
// tail.
func appendStructuralHeader(out []byte, depth int, ckpt [8]byte, distance int, bloomBytes, ha []byte) []byte {
	out = binary.AppendUvarint(out, uint64(maxInt(depth, 0)))
	out = append(out, ckpt[:]...)
	out = append(out, byte(distance-1))
	out = append(out, bloomBytes...)
	out = binary.AppendUvarint(out, uint64(len(ha)))
	return append(out, ha...)
}

// appendChildTail writes a child's tail: its parent's entries, its own (ordinal, end group), its
// parent's delayed entries, then the delayed entries drained for it. The bytes equal repacking the
// decoded parent tail (TestChildTailMatchesRepack).
func appendChildTail(out []byte, parent rawStructuralTail, ordinal int, ends []int, drained []structuralDelayed) []byte {
	out = binary.AppendUvarint(out, parent.n+1)
	out = append(out, parent.entries...)
	out = binary.AppendUvarint(out, uint64(maxInt(ordinal, 0)))
	out = packEndGroup(out, ends, ordinal-1)
	out = binary.AppendUvarint(out, parent.m+uint64(len(drained)))
	out = append(out, parent.delayed...)
	for _, d := range drained {
		out = packStructuralDelayed(out, d)
	}
	return out
}

// structuralDelayedFromServer turns a server span's end-of-request summary
// (varint(children) || varint(count) || count*varint(ordinal)) into a delayed truss stamped with
// the parent's IDs.
func structuralDelayedFromServer(traceID [16]byte, parent [8]byte, summary []byte) (structuralDelayed, bool) {
	k, n := binary.Uvarint(summary)
	if n <= 0 {
		return structuralDelayed{}, false
	}
	summary = summary[n:]
	count, n := binary.Uvarint(summary)
	if n <= 0 || count > uint64(len(summary)) {
		return structuralDelayed{}, false
	}
	summary = summary[n:]
	ends := make([]int, 0, count)
	for i := uint64(0); i < count; i++ {
		v, r := binary.Uvarint(summary)
		if r <= 0 {
			return structuralDelayed{}, false
		}
		summary = summary[r:]
		ends = append(ends, int(v))
	}
	return structuralDelayed{traceID: traceID, parent: parent, children: int(k), ends: ends}, true
}
