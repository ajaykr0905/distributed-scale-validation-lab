package controlapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/runner"
)

func TestRunAndMetrics(t *testing.T) {
	handler := New()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", bytes.NewBufferString(`{"seed":"contract-test","count":64,"workers":2,"max_attempts":3}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response: %d %s", response.Code, response.Body.String())
	}
	var summary runner.Summary
	if err := json.Unmarshal(response.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Generated != 64 || summary.Stored != 64 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, metricsRequest)
	if !strings.Contains(metricsResponse.Body.String(), "scale_lab_runs_total 1") ||
		!strings.Contains(metricsResponse.Body.String(), "scale_lab_results_total 64") {
		t.Fatalf("unexpected metrics: %s", metricsResponse.Body.String())
	}
}

func TestRejectsUnsafeRunSize(t *testing.T) {
	handler := New()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", bytes.NewBufferString(`{"count":100001,"workers":2,"max_attempts":3}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected validation failure, got %d", response.Code)
	}
}

func TestRunRequiresExactlyOneJSONValue(t *testing.T) {
	for _, suffix := range []string{` {}`, ` null`, ` garbage`, ` {`} {
		t.Run(suffix, func(t *testing.T) {
			handler := New()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"count":1}`+suffix))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			metrics := httptest.NewRecorder()
			handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !strings.Contains(metrics.Body.String(), "scale_lab_runs_total 0\n") {
				t.Fatalf("invalid request executed a run: %s", metrics.Body.String())
			}
		})
	}
	response := httptest.NewRecorder()
	New().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader("{\"count\":1} \n\t")))
	if response.Code != http.StatusOK {
		t.Fatalf("trailing whitespace rejected: %d %s", response.Code, response.Body.String())
	}
}

func TestRunBodySizeStatus(t *testing.T) {
	const limit = 16 << 10
	base := `{"count":1}`
	for _, tc := range []struct {
		name string
		body string
		code int
	}{
		{"exact limit", base + strings.Repeat(" ", limit-len(base)), http.StatusOK},
		{"trailing overflow", base + strings.Repeat(" ", limit-len(base)+1), http.StatusRequestEntityTooLarge},
		{"value overflow", `{"seed":"` + strings.Repeat("a", limit) + `","count":1}`, http.StatusRequestEntityTooLarge},
	} {
		for _, streamed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/streamed=%t", tc.name, streamed), func(t *testing.T) {
				request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(tc.body))
				if streamed {
					request.ContentLength = -1
				}
				response := httptest.NewRecorder()
				New().ServeHTTP(response, request)
				if response.Code != tc.code {
					t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
				}
			})
		}
	}
}

func TestRunRequiresJSONObject(t *testing.T) {
	for _, body := range []string{`null`, `[]`, `true`, `42`, `"options"`} {
		t.Run(body, func(t *testing.T) {
			handler := New()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			metrics := httptest.NewRecorder()
			handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !strings.Contains(metrics.Body.String(), "scale_lab_runs_total 0\n") {
				t.Fatalf("non-object request executed a run: %s", metrics.Body.String())
			}
		})
	}
	response := httptest.NewRecorder()
	New().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{}`)))
	var summary runner.Summary
	if err := json.Unmarshal(response.Body.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || summary.Generated != 10_000 || summary.Workers != 4 {
		t.Fatalf("empty object must retain defaults: status=%d summary=%+v", response.Code, summary)
	}
}

func TestRunRejectsFinishedContextBeforeExecution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
		code     int
	}{
		{"canceled", false, http.StatusRequestTimeout},
		{"deadline", true, http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tc.deadline {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			} else {
				cancel()
			}
			defer cancel()
			handler := New()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"count":1}`)).WithContext(ctx)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.code {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
			}
			metrics := httptest.NewRecorder()
			handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !strings.Contains(metrics.Body.String(), "scale_lab_runs_total 0\n") || !strings.Contains(metrics.Body.String(), "scale_lab_failures_total 0\n") {
				t.Fatalf("canceled request executed a run: %s", metrics.Body.String())
			}
		})
	}
}

func TestRunClassifiesExecutionErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code int
	}{
		{"cancel", context.Canceled, 408},
		{"deadline", context.DeadlineExceeded, 504},
		{"wrapped cancel", fmt.Errorf("private details: %w", context.Canceled), 408},
		{"wrapped deadline", fmt.Errorf("private details: %w", context.DeadlineExceeded), 504},
		{"validation", errors.New("count must be between 1 and 100000"), 422},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := newHandler(func(ctx context.Context, options runner.Options) (runner.Summary, error) {
				if ctx == nil || options.Count != 1 {
					t.Fatalf("unexpected execution context or options: %+v", options)
				}
				return runner.Summary{}, tc.err
			})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"count":1}`)))
			if response.Code != tc.code || strings.Contains(response.Body.String(), "private details") {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
			}
			metrics := httptest.NewRecorder()
			handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			for _, metric := range []string{"scale_lab_runs_total 1\n", "scale_lab_failures_total 1\n", "scale_lab_results_total 0\n"} {
				if !strings.Contains(metrics.Body.String(), metric) {
					t.Fatalf("missing %q in metrics: %s", metric, metrics.Body.String())
				}
			}
		})
	}
}
