package controlapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveResponsesAreNotCacheable(t *testing.T) {
	handler := New()
	for _, tc := range []struct {
		method string
		path   string
		body   string
		code   int
	}{
		{http.MethodGet, "/healthz", "", 200},
		{http.MethodGet, "/metrics", "", 200},
		{http.MethodGet, "/missing", "", 404},
		{http.MethodGet, "/api/v1/runs", "", 405},
		{http.MethodPost, "/api/v1/runs", `{"count":1}`, 200},
		{http.MethodPost, "/api/v1/runs", `null`, 400},
	} {
		t.Run(tc.method+tc.path+tc.body, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			if response.Code != tc.code || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d cache=%q body=%s", response.Code, response.Header().Get("Cache-Control"), response.Body.String())
			}
		})
	}
}

func TestHEADLiveResponseHeaders(t *testing.T) {
	server := httptest.NewServer(New())
	defer server.Close()
	for path, contentType := range map[string]string{
		"/healthz": "application/json",
		"/metrics": "text/plain; version=0.0.4",
	} {
		t.Run(path, func(t *testing.T) {
			response, err := server.Client().Head(server.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != 200 || len(body) != 0 || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Type") != contentType {
				t.Fatalf("status=%d headers=%v body=%s", response.StatusCode, response.Header, body)
			}
		})
	}
}
