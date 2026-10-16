package durable

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPRepositoryContextsHaveBoundedLifetime(t *testing.T) {
	type contextKey struct{}
	for _, parentTimeout := range []time.Duration{0, time.Second, time.Minute} {
		for _, request := range durableRequests(t) {
			t.Run(fmt.Sprintf("parent=%s%s", parentTimeout, request.URL.Path), func(t *testing.T) {
				parent := context.WithValue(context.Background(), contextKey{}, "request marker")
				if parentTimeout != 0 {
					var cancel context.CancelFunc
					parent, cancel = context.WithTimeout(parent, parentTimeout)
					defer cancel()
				}
				started := time.Now()
				var operation context.Context
				repository := &contextRepository{check: func(ctx context.Context) error {
					operation = ctx
					deadline, ok := ctx.Deadline()
					if !ok || deadline.After(started.Add(5*time.Second+100*time.Millisecond)) {
						t.Errorf("repository operation is unbounded: deadline=%v present=%t", deadline, ok)
					}
					if parentTimeout == time.Second {
						want, _ := parent.Deadline()
						if !deadline.Equal(want) {
							t.Errorf("shorter caller deadline was replaced: got=%v want=%v", deadline, want)
						}
					} else if deadline.Before(started.Add(4 * time.Second)) {
						t.Errorf("unexpectedly short default deadline: %v", deadline)
					}
					if ctx.Value(contextKey{}) != "request marker" {
						t.Error("request context values lost")
					}
					return nil
				}}
				response := httptest.NewRecorder()
				HTTP(repository, 3, 100).ServeHTTP(response, request.WithContext(parent))
				code := http.StatusOK
				if request.Method == http.MethodPost {
					code = http.StatusAccepted
				}
				if response.Code != code || operation == nil {
					t.Fatalf("status=%d repository context=%v", response.Code, operation)
				}
				if !errors.Is(operation.Err(), context.Canceled) || parent.Err() != nil {
					t.Fatalf("operation resources not released independently: operation=%v parent=%v", operation.Err(), parent.Err())
				}
			})
		}
	}
}

func TestHTTPRepositoryWaitHonorsCallerCancellation(t *testing.T) {
	for _, request := range durableRequests(t) {
		t.Run(request.URL.Path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 1)
			repository := &contextRepository{check: func(ctx context.Context) error {
				entered <- struct{}{}
				<-ctx.Done()
				return fmt.Errorf("private database details: %w", ctx.Err())
			}}
			completed := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				response := httptest.NewRecorder()
				HTTP(repository, 3, 100).ServeHTTP(response, request.WithContext(ctx))
				completed <- response
			}()
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("repository did not start")
			}
			cancel()
			select {
			case response := <-completed:
				if response.Code != 408 || strings.Contains(response.Body.String(), "private database") {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("repository wait ignored caller cancellation")
			}
		})
	}
}
