package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
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

type keyRepository struct {
	fakeRepository
	key string
}

func (r *keyRepository) Submit(ctx context.Context, key string, endpoint model.Endpoint, attempts, capacity int) (Job, bool, error) {
	r.key = key
	return r.fakeRepository.Submit(ctx, key, endpoint, attempts, capacity)
}

func TestHTTPRequiresOneIdempotencyKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		code   int
	}{
		{"missing", nil, 400},
		{"empty", []string{""}, 400},
		{"same twice", []string{"key", "key"}, 400},
		{"different", []string{"first", "second"}, 400},
		{"empty second", []string{"key", ""}, 400},
		{"single", []string{"one-key"}, 202},
		{"literal comma", []string{"one,two"}, 202},
		{"maximum length", []string{strings.Repeat("k", 128)}, 202},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repository := &keyRepository{fakeRepository: fakeRepository{created: true}}
			request := jobRequest(endpointJSON(t))
			request.Header.Del("Idempotency-Key")
			for _, value := range tc.values {
				request.Header.Add("Idempotency-Key", value)
			}
			response := httptest.NewRecorder()
			HTTP(repository, 3, 100).ServeHTTP(response, request)
			if response.Code != tc.code {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.code, response.Body.String())
			}
			if tc.code == 400 && repository.calls != 0 {
				t.Fatalf("ambiguous key reached repository %d times", repository.calls)
			}
			if tc.code == 202 && (repository.calls != 1 || repository.key != tc.values[0]) {
				t.Fatalf("calls=%d submitted key=%q", repository.calls, repository.key)
			}
		})
	}
}

type contextRepository struct {
	check func(context.Context) error
	calls int
}

func (r *contextRepository) Submit(ctx context.Context, _ string, _ model.Endpoint, _, _ int) (Job, bool, error) {
	r.calls++
	return Job{ID: strings.Repeat("a", 64), Status: "pending"}, true, r.check(ctx)
}

func (r *contextRepository) Get(ctx context.Context, _ string) (Job, error) {
	r.calls++
	return Job{Status: "completed"}, r.check(ctx)
}

func (r *contextRepository) Counts(ctx context.Context) (Counts, error) {
	r.calls++
	return Counts{}, r.check(ctx)
}

func durableRequests(t *testing.T) []*http.Request {
	t.Helper()
	return []*http.Request{
		jobRequest(endpointJSON(t)),
		httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+strings.Repeat("a", 64), nil),
		httptest.NewRequest(http.MethodGet, "/healthz", nil),
		httptest.NewRequest(http.MethodGet, "/metrics", nil),
	}
}

func TestHTTPClassifiesRepositoryContextErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code int
	}{
		{"cancel", context.Canceled, 408},
		{"deadline", context.DeadlineExceeded, 504},
		{"wrapped cancel", fmt.Errorf("private details: %w", context.Canceled), 408},
		{"wrapped deadline", fmt.Errorf("private details: %w", context.DeadlineExceeded), 504},
		{"unavailable", errors.New("private details"), 503},
	} {
		for _, request := range durableRequests(t) {
			t.Run(tc.name+request.URL.Path, func(t *testing.T) {
				repository := &contextRepository{check: func(context.Context) error { return tc.err }}
				response := httptest.NewRecorder()
				HTTP(repository, 3, 100).ServeHTTP(response, request)
				if response.Code != tc.code || strings.Contains(response.Body.String(), "private details") || repository.calls != 1 {
					t.Fatalf("status=%d want=%d calls=%d body=%s", response.Code, tc.code, repository.calls, response.Body.String())
				}
			})
		}
	}
}

func TestHTTPRejectsFinishedContextBeforeRepository(t *testing.T) {
	for _, expired := range []bool{false, true} {
		for _, request := range durableRequests(t) {
			t.Run(fmt.Sprintf("expired=%t%s", expired, request.URL.Path), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				code := http.StatusRequestTimeout
				if expired {
					cancel()
					ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
					code = http.StatusGatewayTimeout
				} else {
					cancel()
				}
				defer cancel()
				repository := &contextRepository{check: func(context.Context) error { return nil }}
				response := httptest.NewRecorder()
				HTTP(repository, 3, 100).ServeHTTP(response, request.WithContext(ctx))
				if response.Code != code || repository.calls != 0 {
					t.Fatalf("status=%d want=%d calls=%d", response.Code, code, repository.calls)
				}
			})
		}
	}
}

type cancelOnEOFReader struct {
	reader io.Reader
	cancel context.CancelFunc
}

func (r *cancelOnEOFReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

func TestHTTPRechecksCancellationAfterBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := jobRequest(endpointJSON(t)).WithContext(ctx)
	request.Body = io.NopCloser(&cancelOnEOFReader{reader: strings.NewReader(endpointJSON(t)), cancel: cancel})
	repository := &contextRepository{check: func(context.Context) error { return nil }}
	response := httptest.NewRecorder()
	HTTP(repository, 3, 100).ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout || repository.calls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", response.Code, repository.calls, response.Body.String())
	}
}

func TestHTTPSuccessSurvivesCancellationAtRepositoryReturn(t *testing.T) {
	for _, request := range durableRequests(t) {
		t.Run(request.URL.Path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			repository := &contextRepository{check: func(context.Context) error { cancel(); return nil }}
			response := httptest.NewRecorder()
			HTTP(repository, 3, 100).ServeHTTP(response, request.WithContext(ctx))
			code := http.StatusOK
			if request.Method == http.MethodPost {
				code = http.StatusAccepted
			}
			if response.Code != code || repository.calls != 1 {
				t.Fatalf("successful result lost: status=%d calls=%d body=%s", response.Code, repository.calls, response.Body.String())
			}
		})
	}
}

func TestHTTPAdmissionErrorsDoNotExposeWrappedDetails(t *testing.T) {
	for _, sentinel := range []error{ErrConflict, ErrFull} {
		for _, wrapped := range []error{
			fmt.Errorf("private query and connection details: %w", sentinel),
			errors.Join(sentinel, errors.New("private query and connection details")),
		} {
			t.Run(wrapped.Error(), func(t *testing.T) {
				repository := &fakeRepository{err: wrapped}
				response := httptest.NewRecorder()
				HTTP(repository, 3, 100).ServeHTTP(response, jobRequest(endpointJSON(t)))
				code := http.StatusConflict
				if sentinel == ErrFull {
					code = http.StatusTooManyRequests
					if response.Header().Get("Retry-After") != "1" {
						t.Fatal("missing capacity retry hint")
					}
				}
				var payload map[string]string
				if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if response.Code != code || payload["error"] != sentinel.Error() || repository.calls != 1 {
					t.Fatalf("status=%d body=%s calls=%d", response.Code, response.Body.String(), repository.calls)
				}
				if response.Header().Get("Location") != "" {
					t.Fatal("failed submission advertised a job location")
				}
			})
		}
	}
}
