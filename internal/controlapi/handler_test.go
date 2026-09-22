package controlapi

import (
	"bytes"
	"encoding/json"
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
