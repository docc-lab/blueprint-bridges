package otelcol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// configRetryInterval is the pause between two config discovery attempts.
var configRetryInterval = time.Second

// configWaitLogEvery is the number of failed attempts between two warnings while the agent does not answer.
const configWaitLogEvery = 30

// discoverConfig reads the bridge parameters (checkpoint distances, reverse policy) from the node agent's
// /getFullConfig endpoint. It retries until the agent answers or ctx ends; there is no attempt limit, because
// a bridge processor must never run without its configuration. The agent can come up long after the services
// on its node (a DaemonSet pulling a new image). A service waiting here does not listen yet, so it is not ready
// either.
func discoverConfig(ctx context.Context, client *http.Client, endpoint string) (map[string]interface{}, error) {
	url := fmt.Sprintf("http://%s/getFullConfig", endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("config discovery request: %w", err)
	}
	start := time.Now()
	for attempt := 1; ; attempt++ {
		config, err := getConfig(client, req)
		if err == nil {
			slog.Info("Config discovery successful", "attempt", attempt,
				"waited", time.Since(start).Round(time.Millisecond), "config_keys", len(config))
			return config, nil
		}
		if attempt%configWaitLogEvery == 1 {
			slog.Warn("Waiting for the agent's config", "endpoint", url, "attempt", attempt,
				"waited", time.Since(start).Round(time.Second), "error", err)
		}
		timer := time.NewTimer(configRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("config discovery at %s gave up: %w (last attempt: %v)", url, ctx.Err(), err)
		case <-timer.C:
		}
	}
}

// getConfig makes one discovery attempt.
func getConfig(client *http.Client, req *http.Request) (map[string]interface{}, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body ConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decoding the response: %w", err)
	}
	if body.Config == nil {
		return nil, errors.New("response holds no config")
	}
	return body.Config, nil
}
