//go:build !sb_nolehmer

package otelcol

// SB encodes end-event groups as Lehmer codes. Build with -tags sb_nolehmer for plain lists. The
// choice is made per binary, so all services of a deployment must be built the same way.
const lehmerEnabled = true
