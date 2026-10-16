package controlapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/runner"
)

type Handler struct {
	runs      atomic.Uint64
	failures  atomic.Uint64
	processed atomic.Uint64
	execute   runFunc
}

type runFunc func(context.Context, runner.Options) (runner.Summary, error)

func New() http.Handler {
	return newHandler(runner.Run)
}

func newHandler(execute runFunc) http.Handler {
	handler := &Handler{execute: execute}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.health)
	mux.HandleFunc("GET /metrics", handler.metrics)
	mux.HandleFunc("POST /api/v1/runs", handler.run)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(response, request)
	})
}

func (h *Handler) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) run(response http.ResponseWriter, request *http.Request) {
	if err := request.Context().Err(); err != nil {
		writeRunError(response, err)
		return
	}
	if !supportedJSON(request) {
		writeJSON(response, http.StatusUnsupportedMediaType, map[string]string{"error": "expected uncompressed application/json with UTF-8 encoding"})
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 16<<10)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	options := &runner.Options{Seed: "public-demo", Count: 10_000, Workers: 4, MaxAttempts: 3}
	if err := decoder.Decode(&options); err != nil {
		writeRequestError(response, err, "invalid run request")
		return
	}
	if options == nil {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "run options must be a JSON object"})
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeRequestError(response, err, "exactly one JSON run request is required")
		return
	}

	if err := request.Context().Err(); err != nil {
		writeRunError(response, err)
		return
	}
	summary, err := h.execute(request.Context(), *options)
	h.runs.Add(1)
	if err != nil {
		h.failures.Add(1)
		writeRunError(response, err)
		return
	}
	h.processed.Add(uint64(summary.Stored))
	writeJSON(response, http.StatusOK, summary)
}

func writeRunError(response http.ResponseWriter, err error) {
	status, message := http.StatusUnprocessableEntity, err.Error()
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		status, message = http.StatusGatewayTimeout, "run deadline exceeded"
	case errors.Is(err, context.Canceled):
		status, message = http.StatusRequestTimeout, "request canceled"
	}
	writeJSON(response, status, map[string]string{"error": message})
}

func writeRequestError(response http.ResponseWriter, err error, message string) {
	status := http.StatusBadRequest
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		status = http.StatusRequestEntityTooLarge
		message = "request body exceeds 16 KiB"
	}
	writeJSON(response, status, map[string]string{"error": message})
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
