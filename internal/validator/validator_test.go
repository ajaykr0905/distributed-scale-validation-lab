package validator

import (
	"context"
	"testing"

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
