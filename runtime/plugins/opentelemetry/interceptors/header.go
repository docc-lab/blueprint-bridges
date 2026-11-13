package interceptors

import (
	"encoding/base64"
	"unsafe"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
)

// gRPC sends metadata as HTTP/2 headers, compressed with HPACK. HPACK Huffman-codes a value whenever
// that makes it shorter, and ordinary base64 always gets shorter, so every call would pay for
// Huffman coding on both sides.
//
// headerAlphabet is a base64 alphabet made of the visible ASCII characters with the LONGEST
// Huffman codes (8 to 15 bits for the first 23, 7 for the next 32, 6 for the last 9), so that a
// value written with it is practically never shorter Huffman-coded, and HPACK sends it as plain
// bytes: no Huffman work on either side.  It costs about 23% more bytes on the wire for these two
// values (plain instead of Huffman-coded).  Nothing depends on HPACK's choice: a value it does
// Huffman-code still arrives the same, only slower.
//
// Left out on purpose: space (not allowed at the edges of a header value), the quote characters,
// backslash and backtick (legal in HTTP/2 header values, but they need escaping in logs and tools),
// and '=' (base64's padding character; seeing it inside data would only confuse).
// Order: longest codes first, so the 6-bit group 0 (common in small numbers) gets a long code.
const headerAlphabet = "<{^}$@[]~#>+|!()?&*,;XZ:BCDEFGHIJKLMNOPQRSTUVWYjkqvwxyz-._6/A345"

// headerEncoding writes binary as header text with headerAlphabet, unpadded.  Used for the forward
// carrier (carrier.go) and the reverse context (below).  Its text is printable ASCII, as gRPC requires
// of a header value (and HTTP/1.1 of a field value).
var headerEncoding = base64.NewEncoding(headerAlphabet).WithPadding(base64.NoPadding)

// retCtxHeader writes a reverse context for the response trailer (gRPC) or response header (HTTP).
// The backend keeps it as raw bytes (runtime/core/backend/reversetruss_packed.go), so this is the one
// encoding it gets on its way to the caller; retCtxFromHeader below is the one decoding.  Called by
// the server side (server.go, http_server.go).
func retCtxHeader(retCtx string) string {
	out := make([]byte, headerEncoding.EncodedLen(len(retCtx)))
	headerEncoding.Encode(out, unsafe.Slice(unsafe.StringData(retCtx), len(retCtx)))
	return unsafe.String(unsafe.SliceData(out), len(out)) // out is never written again
}

// retCtxFromHeader turns a trailer or header value back into the reverse context the server's
// backend produced.  Called by the client side (client.go, http_client.go).
//
// Only a traced server writes this value, always from a packed envelope, so anything else is a bug
// on that side.
func retCtxFromHeader(v string) string {
	if v == "" {
		return ""
	}
	raw, err := headerEncoding.DecodeString(v)
	if err != nil {
		backend.InvariantBroken("the reverse context header decodes", err)
	}
	retCtx := unsafe.String(unsafe.SliceData(raw), len(raw)) // raw is never written again
	if !backend.IsPackedRetCtx(retCtx) {
		brokenCount("the reverse context received is a packed envelope", "length", len(raw), 0)
	}
	return retCtx
}
