package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"

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
		if !supportedJSON(r) {
			respond(w, http.StatusUnsupportedMediaType, map[string]string{"error": "expected uncompressed application/json with UTF-8 encoding"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var endpoint model.Endpoint
		if err := decoder.Decode(&endpoint); err != nil {
			requestError(w, err, "invalid endpoint JSON")
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			requestError(w, err, "exactly one JSON endpoint is required")
			return
		}
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 {
			respond(w, http.StatusBadRequest, map[string]string{"error": "exactly one Idempotency-Key header is required"})
			return
		}
		key := keys[0]
		if _, _, _, err := RequestIdentity(key, endpoint); err != nil {
			respond(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if contextError(w, r.Context().Err()) {
			return
		}
		job, created, err := repository.Submit(r.Context(), key, endpoint, maxAttempts, capacity)
		if contextError(w, err) {
			return
		}
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
		if contextError(w, err) {
			return
		}
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
			if contextError(w, err) {
				return
			}
			respond(w, 503, map[string]string{"status": "database unavailable"})
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		c, err := repository.Counts(r.Context())
		if contextError(w, err) {
			return
		}
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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contextError(w, r.Context().Err()) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

var jobID = regexp.MustCompile(`^[a-f0-9]{64}$`)

func contextError(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		respond(w, http.StatusGatewayTimeout, map[string]string{"error": "database operation deadline exceeded; retry submissions with the same key"})
	case errors.Is(err, context.Canceled):
		respond(w, http.StatusRequestTimeout, map[string]string{"error": "request canceled; retry submissions with the same key"})
	default:
		return false
	}
	return true
}

func supportedJSON(r *http.Request) bool {
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) > 1 {
		return false
	}
	if len(contentTypes) == 1 {
		mediaType, params, err := mime.ParseMediaType(contentTypes[0])
		if err != nil || mediaType != "application/json" {
			return false
		}
		if charset, ok := params["charset"]; ok && !strings.EqualFold(charset, "utf-8") {
			return false
		}
	}
	encodings := r.Header.Values("Content-Encoding")
	return len(encodings) == 0 || (len(encodings) == 1 &&
		(strings.TrimSpace(encodings[0]) == "" || strings.EqualFold(strings.TrimSpace(encodings[0]), "identity")))
}

func requestError(w http.ResponseWriter, err error, message string) {
	status := http.StatusBadRequest
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		status = http.StatusRequestEntityTooLarge
		message = "request body exceeds 16 KiB"
	}
	respond(w, status, map[string]string{"error": message})
}

func respond(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
