package otelcol

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
)

var errInvalidReversePolicy = errors.New("invalid reverse checkpoint policy")

const (
	reversePolicyTTL          = "ttl"
	reversePolicyProbability  = "probability"
	reversePolicyInverseDepth = "inverse_depth"
	reversePolicyDepthLinear  = "depth_linear"
	// depth_cubic weights each ancestor of the origin by (d+1)^3, normalized by
	// sum_{j=1..n} j^3 = (n(n+1)/2)^2. The first hop accepts with probability 4n/(n+1)^2 ~ 4/n,
	// so trusses are absorbed near their leaf rather than travelling to the root.
	reversePolicyDepthCubic = "depth_cubic"
)

// reversePolicy decides where a truss travelling back up the call path is emitted. It is read from
// the collector's config before any span starts. The zero value is the TTL policy. The
// probabilistic policies need no extra metadata: every truss carries its leaf's absolute depth.
type reversePolicy struct {
	mode        string
	probability float64
	// passthrough (config key reverse_passthrough, default false): when off, a scheduled checkpoint
	// absorbs every returned truss; when on, it routes them like any other non-root span. The root
	// always absorbs.
	passthrough bool
}

func parseReversePolicy(config map[string]interface{}) (reversePolicy, error) {
	policy := reversePolicy{mode: reversePolicyTTL}
	if value, present := config["reverse_policy"]; present {
		mode, ok := value.(string)
		if !ok {
			return reversePolicy{}, fmt.Errorf("%w: reverse_policy must be a string", errInvalidReversePolicy)
		}
		policy.mode = mode
	}
	if value, present := config["reverse_passthrough"]; present {
		passthrough, ok := value.(bool)
		if !ok {
			return reversePolicy{}, fmt.Errorf("%w: reverse_passthrough must be a boolean", errInvalidReversePolicy)
		}
		policy.passthrough = passthrough
	}
	value, hasProbability := config["reverse_probability"]
	switch policy.mode {
	case reversePolicyTTL, reversePolicyInverseDepth, reversePolicyDepthLinear, reversePolicyDepthCubic:
		if hasProbability {
			return reversePolicy{}, fmt.Errorf("%w: reverse_probability requires reverse_policy: probability", errInvalidReversePolicy)
		}
	case reversePolicyProbability:
		probability, ok := reverseProbabilityValue(value)
		if !hasProbability || !ok {
			return reversePolicy{}, fmt.Errorf("%w: probability policy requires finite reverse_probability in [0,1]", errInvalidReversePolicy)
		}
		policy.probability = probability
	default:
		return reversePolicy{}, fmt.Errorf("%w: reverse_policy must be ttl, probability, inverse_depth, depth_linear, or depth_cubic", errInvalidReversePolicy)
	}
	return policy, nil
}

func reverseProbabilityValue(value interface{}) (float64, bool) {
	var probability float64
	var err error
	switch v := value.(type) {
	case float64:
		probability = v
	case int:
		probability = float64(v)
	case int64:
		probability = float64(v)
	case json.Number:
		probability, err = v.Float64()
	case string:
		probability, err = strconv.ParseFloat(v, 64)
	default:
		return 0, false
	}
	return probability, err == nil && !math.IsNaN(probability) && probability >= 0 && probability <= 1
}

func (p reversePolicy) probabilistic() bool {
	return p.mode != "" && p.mode != reversePolicyTTL
}

// probabilityAt is the probability that the ancestor at depth d emits a truss whose leaf is at
// depth n. It is a per-node conditional probability, not the overall emission weight. Like 1/n,
// depth_linear sums to one over the n receivers but favours deeper ones.
func (p reversePolicy) probabilityAt(d, n uint64) float64 {
	if n == 0 || d >= n {
		return 0 // invalid ancestry
	}
	switch p.mode {
	case reversePolicyProbability:
		return p.probability
	case reversePolicyInverseDepth:
		return 1 / float64(n)
	case reversePolicyDepthLinear:
		// float64 before the arithmetic, so large depths cannot overflow
		return (2 * ((float64(d) + 1) / float64(n))) / (float64(n) + 1)
	case reversePolicyDepthCubic:
		// 4 (d+1)^3 / (n^2 (n+1)^2)
		dd, nn := float64(d)+1, float64(n)
		return 4 * dd * dd * dd / (nn * nn * (nn + 1) * (nn + 1))
	default:
		return 0
	}
}

// terminates reports whether a span absorbs every returned truss: the root always, a scheduled
// checkpoint unless reverse_passthrough is on.
func (p reversePolicy) terminates(scheduled, root bool) bool {
	return root || (scheduled && !p.passthrough)
}

// route splits a reverse context into the part this span emits and the part it passes upstream.
// draw returns a uniform random number in [0,1).
func (p reversePolicy) route(retCtx string, originalCheckpoint bool, depth uint64, draw func() float64) (string, string) {
	if !p.probabilistic() {
		return backend.RouteRetCtx(retCtx, originalCheckpoint)
	}
	return backend.RouteRetCtxWithDecision(retCtx, originalCheckpoint, func(cp backend.ReturnedCheckpoint) bool {
		probability := p.probabilityAt(depth, cp.Depth)
		if !(probability >= 0 && probability <= 1) { // also rejects NaN
			brokenRule("a reverse policy's probability is within [0,1]")
		}
		return probability >= 1 || (probability > 0 && draw() < probability)
	})
}
