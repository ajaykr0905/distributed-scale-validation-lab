package validator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

type Validator interface {
	Validate(context.Context, model.Endpoint) (model.ValidationResult, error)
}

type Basic struct {
	Now func() time.Time
}

// ValidateEndpoint checks the minimum domain contract shared by queue workers
// and validators. Vocabulary values remain extensible for synthetic datasets.
func ValidateEndpoint(endpoint model.Endpoint) error {
	for _, field := range []struct{ name, value string }{
		{"id", endpoint.ID}, {"tenant_id", endpoint.TenantID},
		{"region", endpoint.Region}, {"platform", endpoint.Platform},
		{"expected_version", endpoint.ExpectedVersion},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("endpoint %s is required", field.name)
		}
	}
	if endpoint.Sequence < 0 {
		return fmt.Errorf("endpoint sequence must be nonnegative")
	}
	return nil
}

// ValidateResult prevents a successful validator call from persisting an empty
// result or attributing a result to a different endpoint.
func ValidateResult(endpoint model.Endpoint, result model.ValidationResult) error {
	if result.EndpointID != endpoint.ID {
		return fmt.Errorf("validation result endpoint does not match input")
	}
	if strings.TrimSpace(result.Status) == "" || strings.TrimSpace(result.Fingerprint) == "" || result.CheckedAt.IsZero() {
		return fmt.Errorf("validation result requires status, fingerprint, and checked time")
	}
	return nil
}

func (v Basic) Validate(ctx context.Context, endpoint model.Endpoint) (model.ValidationResult, error) {
	if err := ctx.Err(); err != nil {
		return model.ValidationResult{}, err
	}
	if err := ValidateEndpoint(endpoint); err != nil {
		return model.ValidationResult{}, err
	}
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s",
		endpoint.ID,
		endpoint.Region,
		endpoint.Platform,
		endpoint.ExpectedVersion,
	)))
	result := model.ValidationResult{
		EndpointID:  endpoint.ID,
		Status:      "valid",
		Fingerprint: fmt.Sprintf("%x", digest[:12]),
		CheckedAt:   now().UTC(),
	}
	return result, ValidateResult(endpoint, result)
}
