package otelcol

import (
	"github.com/blueprint-uservices/blueprint/runtime/plugins/bloom"
)

// Baggage keys under which the bridges store their per-span state.
const (
	BAG_BLOOM_FILTER       = "__bag.bf"
	BAG_HASH_ARRAY         = "__bag.ha"
	BAG_ORDINAL_ARRAY      = "__bag.oa"
	BAG_END_EVENTS         = "__bag.ee"
	BAG_DELAYED_END_EVENTS = "__bag.dee"
)

// Span attribute keys for the ancestry tags.
const (
	AncestryModeKey  = "ancestry_mode"
	AncestryKey      = "ancestry"
	AncestryExtraKey = "ancestry2"
)

// AncestryMode selects how a span's ancestry is encoded. The values are single bytes so the mode
// costs one byte on the wire.
type AncestryMode string

var (
	AncestryModePB     AncestryMode = AncestryMode(string([]byte{0x00}))
	AncestryModeHash   AncestryMode = AncestryMode(string([]byte{0x01}))
	AncestryModeHybrid AncestryMode = AncestryMode(string([]byte{0x02}))
)

// singleOTLPClient exports high- and low-priority spans over one OTLP client. When false, low-priority
// spans get their own client and endpoint.
const singleOTLPClient = true

// Bloom filter parameters: m is the bit array size, k the number of hash functions.
var (
	BloomFilterM uint64 = 10
	BloomFilterK uint   = 7
)

// ConfigResponse is the body returned by the collector's config discovery endpoint.
type ConfigResponse struct {
	Config map[string]interface{} `json:"config"`
}

// serializeBloomFilter encodes only the filter's bit array, as base64.
func serializeBloomFilter(bf *bloom.BloomFilter) (string, error) {
	return bf.Serialize(), nil
}

// deserializeBloomFilter is the inverse of serializeBloomFilter.
func deserializeBloomFilter(serialized string) (*bloom.BloomFilter, error) {
	return bloom.Deserialize(serialized, BloomFilterM, BloomFilterK)
}

func createEmptyBloomFilter() *bloom.BloomFilter {
	return bloom.New(BloomFilterM, BloomFilterK)
}

// getBaggageKeys lists a baggage map's keys, for log messages.
func getBaggageKeys(baggage map[string]string) []string {
	keys := make([]string, 0, len(baggage))
	for k := range baggage {
		keys = append(keys, k)
	}
	return keys
}
