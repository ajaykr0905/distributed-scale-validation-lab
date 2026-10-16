package controlapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
