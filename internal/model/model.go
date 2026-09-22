package model

import "time"

// Endpoint is a generated lab entity. It intentionally contains no customer or
// employer data.
type Endpoint struct {
	ID              string `json:"id"`
	TenantID        string `json:"tenant_id"`
	Region          string `json:"region"`
	Platform        string `json:"platform"`
	ExpectedVersion string `json:"expected_version"`
	Sequence        int    `json:"sequence"`
}

// ValidationResult is the durable, idempotent output of processing one message.
type ValidationResult struct {
	MessageID   string
	EndpointID  string
	Status      string
	Fingerprint string
	CheckedAt   time.Time
}
