package validator

import (
	"context"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

func validEndpoint() model.Endpoint {
	return model.Endpoint{ID: "endpoint", TenantID: "tenant", Region: "custom-region", Platform: "custom-platform", ExpectedVersion: "v1"}
}

func TestBasicRejectsIncompleteEndpoints(t *testing.T) {
	for _, mutate := range []func(*model.Endpoint){
		func(e *model.Endpoint) { e.ID = "" }, func(e *model.Endpoint) { e.TenantID = " " },
		func(e *model.Endpoint) { e.Region = "" }, func(e *model.Endpoint) { e.Platform = "" },
		func(e *model.Endpoint) { e.ExpectedVersion = "" }, func(e *model.Endpoint) { e.Sequence = -1 },
	} {
		endpoint := validEndpoint()
		mutate(&endpoint)
		if _, err := (Basic{}).Validate(context.Background(), endpoint); err == nil {
			t.Fatalf("accepted %+v", endpoint)
		}
	}
	if _, err := (Basic{}).Validate(context.Background(), validEndpoint()); err != nil {
		t.Fatal(err)
	}
}

func TestResultContractRejectsMissingOrMismatchedOutput(t *testing.T) {
	endpoint := validEndpoint()
	valid, err := (Basic{}).Validate(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*model.ValidationResult){
		func(r *model.ValidationResult) { r.EndpointID = "other" },
		func(r *model.ValidationResult) { r.Status = " " },
		func(r *model.ValidationResult) { r.Fingerprint = "" },
		func(r *model.ValidationResult) { r.CheckedAt = time.Time{} },
	} {
		result := valid
		mutate(&result)
		if err := ValidateResult(endpoint, result); err == nil {
			t.Fatalf("accepted %+v", result)
		}
	}
	if _, err := (Basic{Now: func() time.Time { return time.Time{} }}).Validate(context.Background(), endpoint); err == nil {
		t.Fatal("accepted zero clock time")
	}
}
