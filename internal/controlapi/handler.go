package controlapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/runner"
)

type Handler struct {
	runs      atomic.Uint64
	failures  atomic.Uint64
	processed atomic.Uint64
}

func New() http.Handler {
	handler := &Handler{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.health)
	mux.HandleFunc("GET /metrics", handler.metrics)
	mux.HandleFunc("POST /api/v1/runs", handler.run)
	return mux
}

func (h *Handler) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) run(response http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(response, request.Body, 16<<10)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	options := runner.Options{Seed: "public-demo", Count: 10_000, Workers: 4, MaxAttempts: 3}
	if err := decoder.Decode(&options); err != nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid run request"})
		return
	}

	summary, err := runner.Run(request.Context(), options)
	h.runs.Add(1)
	if err != nil {
		h.failures.Add(1)
		writeJSON(response, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	h.processed.Add(uint64(summary.Stored))
	writeJSON(response, http.StatusOK, summary)
}

func (h *Handler) metrics(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(response,
		"# TYPE scale_lab_runs_total counter\nscale_lab_runs_total %d\n"+
			"# TYPE scale_lab_failures_total counter\nscale_lab_failures_total %d\n"+
			"# TYPE scale_lab_results_total counter\nscale_lab_results_total %d\n",
		h.runs.Load(), h.failures.Load(), h.processed.Load(),
	)
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}
