package generator

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
)

var (
	regions   = [...]string{"ap-south", "ap-southeast", "eu-west", "us-central"}
	platforms = [...]string{"linux", "windows", "container", "edge-os"}
)

// Generate creates the same ordered synthetic dataset for the same seed and
// count. SHA-256 keeps the output independent of pseudo-random library changes.
func Generate(seed string, count int) ([]model.Endpoint, error) {
	if seed == "" {
		return nil, errors.New("seed must not be empty")
	}
	if count < 0 {
		return nil, errors.New("count must not be negative")
	}

	endpoints := make([]model.Endpoint, count)
	for i := range endpoints {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", seed, i)))
		selector := binary.BigEndian.Uint64(digest[:8])
		endpoints[i] = model.Endpoint{
			ID:              fmt.Sprintf("syn-endpoint-%x", digest[:8]),
			TenantID:        fmt.Sprintf("syn-tenant-%02d", selector%32),
			Region:          regions[(selector>>8)%uint64(len(regions))],
			Platform:        platforms[(selector>>16)%uint64(len(platforms))],
			ExpectedVersion: fmt.Sprintf("v%d.%d", 1+(selector>>24)%4, (selector>>32)%10),
			Sequence:        i,
		}
	}
	return endpoints, nil
}
