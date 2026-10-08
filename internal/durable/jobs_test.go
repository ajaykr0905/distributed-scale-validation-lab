package durable

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

func endpoint() model.Endpoint {
	return model.Endpoint{ID: "synthetic-1", TenantID: "synthetic", Region: "ap-south", Platform: "linux", ExpectedVersion: "v1"}
}

func TestRequestIdentity(t *testing.T) {
	a, hash, payload, err := RequestIdentity("request-1", endpoint())
	if err != nil || len(a) != 64 || len(hash) != 64 {
		t.Fatalf("identity=%q hash=%q err=%v", a, hash, err)
	}
	b, hash2, payload2, err := RequestIdentity("request-1", endpoint())
	if err != nil || a != b || hash != hash2 || string(payload) != string(payload2) {
		t.Fatal("not deterministic")
	}
	changed := endpoint()
	changed.ExpectedVersion = "v2"
	c, changedHash, _, _ := RequestIdentity("request-1", changed)
	if c != a || changedHash == hash {
		t.Fatal("payload change did not preserve ID/change fingerprint")
	}
	for _, key := range []string{"", " spaces ", "with space", "\n", strings.Repeat("x", 129)} {
		if _, _, _, err := RequestIdentity(key, endpoint()); err == nil {
			t.Errorf("accepted key %q", key)
		}
	}
	missing := endpoint()
	missing.ID = ""
	if _, _, _, err := RequestIdentity("key", missing); err == nil {
		t.Fatal("accepted missing endpoint")
	}
}

type fakeRepository struct {
	err     error
	created bool
	calls   int
}

func (f *fakeRepository) Submit(context.Context, string, model.Endpoint, int, int) (Job, bool, error) {
	f.calls++
	return Job{ID: strings.Repeat("a", 64), Status: "pending"}, f.created, f.err
}
func (f *fakeRepository) Get(context.Context, string) (Job, error) {
	return Job{Status: "completed"}, f.err
}
func (f *fakeRepository) Counts(context.Context) (Counts, error) {
	return Counts{Accepted: 4, Completed: 3, Pending: 1}, f.err
}

func TestAdmissionHTTP(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		created bool
		code    int
	}{{"accepted", nil, true, 202}, {"replay", nil, false, 200}, {"conflict", ErrConflict, false, 409}, {"full", ErrFull, false, 429}, {"unavailable", errors.New("secret database error"), false, 503}} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{err: tc.err, created: tc.created}
			body, _ := json.Marshal(endpoint())
			req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(string(body)))
			req.Header.Set("Idempotency-Key", "request-1")
			res := httptest.NewRecorder()
			HTTP(repo, 3, 100).ServeHTTP(res, req)
			if res.Code != tc.code {
				t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}
			if strings.Contains(res.Body.String(), "secret") {
				t.Fatal("leaked SQL details")
			}
			if tc.code == 429 && res.Header().Get("Retry-After") == "" {
				t.Fatal("missing backpressure hint")
			}
			if tc.code == 202 && !strings.HasPrefix(res.Header().Get("Location"), "/api/v1/jobs/") {
				t.Fatal("missing job location")
			}
		})
	}
}

func TestHTTPRejectsMalformedAndOversizeBodies(t *testing.T) {
	body, _ := json.Marshal(endpoint())
	for _, value := range []string{`{}`, `{"unknown":1}`, string(body) + ` {}`, strings.Repeat("x", 17000)} {
		repo := &fakeRepository{}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(value))
		req.Header.Set("Idempotency-Key", "key")
		res := httptest.NewRecorder()
		HTTP(repo, 3, 100).ServeHTTP(res, req)
		if res.Code != 400 || repo.calls != 0 {
			t.Fatalf("status=%d calls=%d", res.Code, repo.calls)
		}
	}
}

func TestHTTPStatusMetricsAndReadiness(t *testing.T) {
	repo := &fakeRepository{}
	for _, path := range []string{"/healthz", "/metrics", "/api/v1/jobs/" + strings.Repeat("a", 64)} {
		res := httptest.NewRecorder()
		HTTP(repo, 3, 100).ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
		if res.Code != 200 {
			t.Errorf("%s: %d", path, res.Code)
		}
	}
	for _, err := range []error{ErrNotFound, errors.New("database down")} {
		repo.err = err
		res := httptest.NewRecorder()
		HTTP(repo, 3, 100).ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+strings.Repeat("a", 64), nil))
		want := 503
		if errors.Is(err, ErrNotFound) {
			want = 404
		}
		if res.Code != want {
			t.Fatalf("got %d want %d", res.Code, want)
		}
	}
}
