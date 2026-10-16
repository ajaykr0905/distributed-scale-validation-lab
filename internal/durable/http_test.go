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
