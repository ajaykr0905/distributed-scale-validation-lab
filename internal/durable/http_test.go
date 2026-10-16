package durable

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func endpointJSON(t *testing.T) string {
	t.Helper()
	body, err := json.Marshal(endpoint())
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func jobRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", "contract-key")
	return request
}

func TestHTTPBodySizeStatus(t *testing.T) {
	const limit = 16 << 10
	base := endpointJSON(t)
	for _, tc := range []struct {
		name string
		body string
		code int
	}{
		{"exact limit", base + strings.Repeat(" ", limit-len(base)), http.StatusAccepted},
		{"trailing overflow", base + strings.Repeat(" ", limit-len(base)+1), http.StatusRequestEntityTooLarge},
		{"value overflow", strings.Replace(base, "synthetic-1", strings.Repeat("a", limit), 1), http.StatusRequestEntityTooLarge},
	} {
		for _, streamed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/streamed=%t", tc.name, streamed), func(t *testing.T) {
				repository := &fakeRepository{created: true}
				request := jobRequest(tc.body)
				if streamed {
					request.ContentLength = -1
				}
				response := httptest.NewRecorder()
				HTTP(repository, 3, 100).ServeHTTP(response, request)
				if response.Code != tc.code {
					t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
				}
				if tc.code != http.StatusAccepted && repository.calls != 0 {
					t.Fatalf("oversize request reached repository %d times", repository.calls)
				}
			})
		}
	}
}

func TestHTTPRepresentationHeaders(t *testing.T) {
	body := endpointJSON(t)
	for _, tc := range []struct {
		name   string
		header http.Header
		code   int
	}{
		{"omitted", nil, 202},
		{"json", http.Header{"Content-Type": {"application/json"}}, 202},
		{"utf8", http.Header{"Content-Type": {"Application/JSON; charset=UTF-8"}}, 202},
		{"identity", http.Header{"Content-Encoding": {"identity"}}, 202},
		{"text", http.Header{"Content-Type": {"text/plain"}}, 415},
		{"malformed", http.Header{"Content-Type": {"application/json; charset"}}, 415},
		{"empty", http.Header{"Content-Type": {""}}, 415},
		{"other charset", http.Header{"Content-Type": {"application/json; charset=utf-16"}}, 415},
		{"duplicate", http.Header{"Content-Type": {"application/json", "text/plain"}}, 415},
		{"gzip", http.Header{"Content-Encoding": {"gzip"}}, 415},
		{"encoding list", http.Header{"Content-Encoding": {"identity", "gzip"}}, 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repository := &fakeRepository{created: true}
			request := jobRequest(body)
			for name, values := range tc.header {
				request.Header[name] = values
			}
			response := httptest.NewRecorder()
			HTTP(repository, 3, 100).ServeHTTP(response, request)
			if response.Code != tc.code {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
			}
			if tc.code == 415 && repository.calls != 0 {
				t.Fatalf("unsupported representation reached repository %d times", repository.calls)
			}
		})
	}
}
