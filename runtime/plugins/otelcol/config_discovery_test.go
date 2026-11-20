package otelcol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
)

var discoveryTestConfig = map[string]interface{}{"cpd_min": 2, "cpd_max": 6, "reverse_policy": "inverse_depth"}

func fastConfigRetry(t *testing.T) {
	old := configRetryInterval
	configRetryInterval = 5 * time.Millisecond
	t.Cleanup(func() { configRetryInterval = old })
}

func serveConfig(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"config": discoveryTestConfig})
}

// lateAgent returns a free local address and starts serving the config there after delay, the way a node agent
// comes up after the services on its node. Until then connections are refused.
func lateAgent(t *testing.T, delay time.Duration) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serveConfig(w) })}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-time.After(delay):
		case <-stop:
			return
		}
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("agent listen: %v", err)
			return
		}
		_ = server.Serve(l)
	}()
	t.Cleanup(func() { close(stop); _ = server.Close(); <-done })
	return addr
}

func TestDiscoverConfigWaitsForLateAgent(t *testing.T) {
	fastConfigRetry(t)
	addr := lateAgent(t, 200*time.Millisecond)
	start := time.Now()
	config, err := discoverConfig(context.Background(), &http.Client{Timeout: time.Second}, addr)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 200*time.Millisecond || config["cpd_max"] != 6.0 {
		t.Fatalf("returned after %v with %v", time.Since(start), config)
	}
}

func TestDiscoverConfigRetriesBadResponses(t *testing.T) {
	fastConfigRetry(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			_, _ = w.Write([]byte(`{}`))
		case 3:
			_, _ = w.Write([]byte(`{"config":`))
		default:
			serveConfig(w)
		}
	}))
	defer server.Close()
	config, err := discoverConfig(context.Background(), server.Client(), server.Listener.Addr().String())
	if err != nil || requests.Load() != 4 || config["cpd_min"] != 2.0 {
		t.Fatalf("after %d requests: %v, %v", requests.Load(), config, err)
	}
}

func TestDiscoverConfigEndsWithContext(t *testing.T) {
	fastConfigRetry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	addr := lateAgent(t, time.Hour)
	if _, err := discoverConfig(ctx, &http.Client{Timeout: time.Second}, addr); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}

// The bridge processors start only with the agent's config: they wait for an agent that comes up late, and fail
// rather than fall back to defaults (every span a checkpoint) when the wait ends.
func TestBridgeProcessorsNeedTheAgentsConfig(t *testing.T) {
	fastConfigRetry(t)
	oldM, oldK := BloomFilterM, BloomFilterK
	defer func() { BloomFilterM, BloomFilterK = oldM, oldK; backend.SetRTBloomParams(oldM, oldK) }()

	type bridge struct {
		rng      checkpointRange
		metrics  func()
		shutdown func(context.Context) error
	}
	constructors := map[string]func(ctx context.Context, endpoint, port string) (bridge, error){
		"pb": func(ctx context.Context, endpoint, port string) (bridge, error) {
			p, err := NewPathBridgeProcessor(ctx, endpoint, port)
			if err != nil {
				return bridge{}, err
			}
			return bridge{p.checkpointRange, p.logMetrics, p.Shutdown}, nil
		},
		"cgpb": func(ctx context.Context, endpoint, port string) (bridge, error) {
			p, err := NewCallGraphBridgeProcessor(ctx, endpoint, port)
			if err != nil {
				return bridge{}, err
			}
			return bridge{p.checkpointRange, p.logMetrics, p.Shutdown}, nil
		},
		"sb": func(ctx context.Context, endpoint, port string) (bridge, error) {
			p, err := NewStructuralBridgeProcessor(ctx, endpoint, port)
			if err != nil {
				return bridge{}, err
			}
			return bridge{p.checkpointRange, p.logMetrics, p.Shutdown}, nil
		},
	}
	for kind, construct := range constructors {
		t.Run(kind+"/late agent", func(t *testing.T) {
			_, port, _ := net.SplitHostPort(lateAgent(t, 300*time.Millisecond))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			b, err := construct(ctx, "127.0.0.1:4317", port)
			if err != nil {
				t.Fatal(err)
			}
			defer b.shutdown(context.Background())
			if b.rng != (checkpointRange{2, 6}) {
				t.Fatalf("started with %+v, not the agent's 2..6", b.rng)
			}
			// The metrics line carries the configuration for the experiment runner's check.
			var out bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
			b.metrics()
			slog.SetDefault(old)
			if line := out.String(); !strings.Contains(line, kind+"_processor_metrics") ||
				!strings.Contains(line, " cpd_min=2 cpd_max=6") {
				t.Fatalf("metrics line without the configuration: %s", line)
			}
		})
		t.Run(kind+"/no agent", func(t *testing.T) {
			_, port, _ := net.SplitHostPort(lateAgent(t, time.Hour))
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if b, err := construct(ctx, "127.0.0.1:4317", port); err == nil {
				b.shutdown(context.Background())
				t.Fatal("started without a config")
			}
		})
	}
}
