package validator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

type Validator interface {
	Validate(context.Context, model.Endpoint) (model.ValidationResult, error)
}

type Basic struct {
	Now func() time.Time
}

func (v Basic) Validate(ctx context.Context, endpoint model.Endpoint) (model.ValidationResult, error) {
	if err := ctx.Err(); err != nil {
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
	return model.ValidationResult{
		EndpointID:  endpoint.ID,
		Status:      "valid",
		Fingerprint: fmt.Sprintf("%x", digest[:12]),
		CheckedAt:   now().UTC(),
	}, nil
}
