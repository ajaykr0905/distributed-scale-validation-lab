package durable

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPLiveResponsesAreNotCacheable(t *testing.T) {
	for _, repositoryError := range []error{nil, ErrNotFound, errors.New("database down")} {
		for _, request := range append(durableRequests(t),
			httptest.NewRequest(http.MethodGet, "/missing", nil),
			httptest.NewRequest(http.MethodDelete, "/api/v1/jobs/"+strings.Repeat("a", 64), nil),
		) {
			repository := &fakeRepository{err: repositoryError, created: true}
			response := httptest.NewRecorder()
			HTTP(repository, 3, 100).ServeHTTP(response, request)
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("%s %s status=%d error=%v cache=%q", request.Method, request.URL.Path, response.Code, repositoryError, response.Header().Get("Cache-Control"))
			}
		}
	}
}

func TestHTTPHEADLiveResponseHeaders(t *testing.T) {
	for _, repositoryError := range []error{nil, ErrNotFound} {
		server := httptest.NewServer(HTTP(&fakeRepository{err: repositoryError}, 3, 100))
		for _, path := range []string{"/healthz", "/metrics", "/api/v1/jobs/" + strings.Repeat("a", 64)} {
			response, err := server.Client().Head(server.URL + path)
			if err != nil {
				server.Close()
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			code, contentType := http.StatusOK, "application/json"
			if repositoryError != nil {
				code = http.StatusServiceUnavailable
				if strings.HasPrefix(path, "/api/v1/jobs/") {
					code = http.StatusNotFound
				}
			} else if path == "/metrics" {
				contentType = "text/plain; version=0.0.4"
			}
			if readErr != nil || response.StatusCode != code || len(body) != 0 || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Type") != contentType {
				t.Errorf("%s status=%d headers=%v body=%s err=%v", path, response.StatusCode, response.Header, body, readErr)
			}
		}
		server.Close()
	}
}
