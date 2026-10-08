package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

type Repository interface {
	Submit(context.Context, string, model.Endpoint, int, int) (Job, bool, error)
	Get(context.Context, string) (Job, error)
	Counts(context.Context) (Counts, error)
}

func HTTP(repository Repository, maxAttempts, capacity int) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var endpoint model.Endpoint
		if err := decoder.Decode(&endpoint); err != nil {
			respond(w, 400, map[string]string{"error": "invalid endpoint JSON"})
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			respond(w, 400, map[string]string{"error": "exactly one JSON endpoint is required"})
			return
		}
		if _, _, _, err := RequestIdentity(r.Header.Get("Idempotency-Key"), endpoint); err != nil {
			respond(w, 400, map[string]string{"error": err.Error()})
			return
		}
		job, created, err := repository.Submit(r.Context(), r.Header.Get("Idempotency-Key"), endpoint, maxAttempts, capacity)
		switch {
		case errors.Is(err, ErrConflict):
			respond(w, 409, map[string]string{"error": err.Error()})
		case errors.Is(err, ErrFull):
			w.Header().Set("Retry-After", "1")
			respond(w, 429, map[string]string{"error": err.Error()})
		case err != nil:
			respond(w, 503, map[string]string{"error": "database temporarily unavailable; retry with the same key"})
		default:
			w.Header().Set("Location", "/api/v1/jobs/"+job.ID)
			status := http.StatusOK
			if created {
				status = http.StatusAccepted
			}
			respond(w, status, job)
		}
	})
	mux.HandleFunc("GET /api/v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !jobID.MatchString(id) {
			respond(w, 404, map[string]string{"error": "job not found"})
			return
		}
		job, err := repository.Get(r.Context(), id)
		if errors.Is(err, ErrNotFound) {
			respond(w, 404, map[string]string{"error": "job not found"})
			return
		}
		if err != nil {
			respond(w, 503, map[string]string{"error": "database temporarily unavailable"})
			return
		}
		respond(w, 200, job)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if _, err := repository.Counts(r.Context()); err != nil {
			respond(w, 503, map[string]string{"status": "database unavailable"})
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		c, err := repository.Counts(r.Context())
		if err != nil {
			respond(w, 503, map[string]string{"error": "database temporarily unavailable"})
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, metric := range []struct {
			name  string
			value int64
		}{
			{"accepted", c.Accepted}, {"completed", c.Completed}, {"failed", c.Failed}, {"pending", c.Pending}, {"retrying", c.Retrying}, {"outbox_pending", c.OutboxPending}, {"retries", c.Retries}, {"duplicates", c.Duplicates}, {"results", c.Results},
		} {
			_, _ = fmt.Fprintf(w, "# TYPE scale_lab_jobs_%s gauge\nscale_lab_jobs_%s %d\n", metric.name, metric.name, metric.value)
		}
	})
	return mux
}

var jobID = regexp.MustCompile(`^[a-f0-9]{64}$`)

func respond(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
