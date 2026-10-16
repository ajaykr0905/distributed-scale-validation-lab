package controlapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunRepresentationHeaders(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header http.Header
		code   int
	}{
		{"omitted", nil, 200},
		{"json", http.Header{"Content-Type": {"application/json"}}, 200},
		{"utf8", http.Header{"Content-Type": {"Application/JSON; charset=UTF-8"}}, 200},
		{"identity", http.Header{"Content-Encoding": {"identity"}}, 200},
		{"text", http.Header{"Content-Type": {"text/plain"}}, 415},
		{"malformed", http.Header{"Content-Type": {"application/json; charset"}}, 415},
		{"empty", http.Header{"Content-Type": {""}}, 415},
		{"other charset", http.Header{"Content-Type": {"application/json; charset=utf-16"}}, 415},
		{"duplicate", http.Header{"Content-Type": {"application/json", "text/plain"}}, 415},
		{"gzip", http.Header{"Content-Encoding": {"gzip"}}, 415},
		{"encoding list", http.Header{"Content-Encoding": {"identity", "gzip"}}, 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := New()
			request := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"count":1}`))
			for name, values := range tc.header {
				request.Header[name] = values
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.code {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
			}
			if tc.code == 415 {
				metrics := httptest.NewRecorder()
				handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				if !strings.Contains(metrics.Body.String(), "scale_lab_runs_total 0\n") {
					t.Fatalf("unsupported representation executed a run: %s", metrics.Body.String())
				}
			}
		})
	}
}
