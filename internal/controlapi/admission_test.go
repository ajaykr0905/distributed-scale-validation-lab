package controlapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/runner"
)

func TestRunAdmissionBoundsWorkAndKeepsProbesAvailable(t *testing.T) {
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int64
	handler := newHandler(func(ctx context.Context, _ runner.Options) (runner.Summary, error) {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
			return runner.Summary{Stored: 1}, nil
		case <-ctx.Done():
			return runner.Summary{}, ctx.Err()
		}
	})
	invoke := func(method, path, body string) <-chan *httptest.ResponseRecorder {
		completed := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
			completed <- response
		}()
		return completed
	}
	await := func(completed <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
		t.Helper()
		select {
		case response := <-completed:
			return response
		case <-time.After(2 * time.Second):
			t.Fatal("request blocked behind active runs")
			return nil
		}
	}
	active := []<-chan *httptest.ResponseRecorder{
		invoke(http.MethodPost, "/api/v1/runs", `{"count":1}`),
		invoke(http.MethodPost, "/api/v1/runs", `{"count":1}`),
	}
	for range active {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("expected two admitted runs")
		}
	}
	response := await(invoke(http.MethodPost, "/api/v1/runs", `{"count":1}`))
	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "1" || calls.Load() != 2 {
		t.Fatalf("status=%d headers=%v executions=%d", response.Code, response.Header(), calls.Load())
	}
	for _, path := range []string{"/healthz", "/metrics"} {
		if response := await(invoke(http.MethodGet, path, "")); response.Code != 200 {
			t.Fatalf("probe %s failed: %d", path, response.Code)
		}
	}
	if response := await(invoke(http.MethodPost, "/api/v1/runs", `null`)); response.Code != 400 {
		t.Fatalf("busy API lost input validation: %d", response.Code)
	}
	other := newHandler(func(context.Context, runner.Options) (runner.Summary, error) { return runner.Summary{}, nil })
	otherResponse := httptest.NewRecorder()
	other.ServeHTTP(otherResponse, httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"count":1}`)))
	if otherResponse.Code != 200 {
		t.Fatalf("admission leaked across handler instances: %d", otherResponse.Code)
	}
	unblock()
	for _, completed := range active {
		if response := await(completed); response.Code != 200 {
			t.Fatalf("admitted run failed: %d", response.Code)
		}
	}
	if response := await(invoke(http.MethodPost, "/api/v1/runs", `{"count":1}`)); response.Code != 200 {
		t.Fatalf("admission did not reopen: %d", response.Code)
	}
	metrics := await(invoke(http.MethodGet, "/metrics", ""))
	for _, metric := range []string{"scale_lab_runs_total 3\n", "scale_lab_failures_total 0\n", "scale_lab_results_total 3\n"} {
		if !strings.Contains(metrics.Body.String(), metric) {
			t.Fatalf("rejected request changed run accounting: %s", metrics.Body.String())
		}
	}
}

func TestRunAdmissionReleasesAfterExecutionErrors(t *testing.T) {
	for _, runError := range []error{errors.New("invalid run"), context.Canceled, context.DeadlineExceeded} {
		t.Run(runError.Error(), func(t *testing.T) {
			calls := 0
			handler := newHandler(func(context.Context, runner.Options) (runner.Summary, error) {
				calls++
				if calls <= 3 {
					return runner.Summary{}, runError
				}
				return runner.Summary{}, nil
			})
			for attempt := range 4 {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"count":1}`)))
				if response.Code == 429 || (attempt == 3 && response.Code != 200) {
					t.Fatalf("admission slot leaked: attempt=%d status=%d", attempt, response.Code)
				}
			}
		})
	}
}
