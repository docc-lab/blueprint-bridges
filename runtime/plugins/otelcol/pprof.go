package otelcol

// Optional pprof endpoint for profiling a deployed service. Nothing is served unless BRIDGES_PPROF
// is set:
//
//	BRIDGES_PPROF=:6060
//	kubectl port-forward <pod> 6060:6060
//	go tool pprof -seconds 30 http://localhost:6060/debug/pprof/profile

import (
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
)

// PprofEnv names the listen address for the debug server, e.g. ":6060".
const PprofEnv = "BRIDGES_PPROF"

func init() {
	address := os.Getenv(PprofEnv)
	if address == "" {
		return
	}
	go func() {
		// net/http/pprof registers its handlers on the default mux.
		server := &http.Server{Addr: address, Handler: http.DefaultServeMux}
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("pprof listener stopped", "addr", address, "err", err)
		}
	}()
	slog.Info("pprof listener started", "addr", address)
}
