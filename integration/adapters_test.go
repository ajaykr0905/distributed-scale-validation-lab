//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/model"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/queue"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/store"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/validator"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/worker"
)

func TestRabbitMQAndPostgreSQLAdapters(t *testing.T) {
	if os.Getenv("SCALE_LAB_INTEGRATION") != "1" {
		t.Skip("set SCALE_LAB_INTEGRATION=1 after starting the dedicated Compose profile")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	postgresURL := getenv("SCALE_LAB_POSTGRES_URL", "postgres://scale_lab:local-development-only@127.0.0.1:5432/scale_lab?sslmode=disable")
	rabbitURL := getenv("SCALE_LAB_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
	resultStore, err := store.OpenPostgreSQL(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resultStore.Close()
	if err := resultStore.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	queueName := "scale-lab-integration-" + time.Now().UTC().Format("20060102150405")
	broker, err := queue.NewRabbitMQ(ctx, rabbitURL, queueName)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

	endpoint := model.Endpoint{ID: queueName, TenantID: "synthetic", Region: "ap-south", Platform: "linux", ExpectedVersion: "v1"}
	payload, err := json.Marshal(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	messageID := queueName + ":validation:v1"
	if err := broker.Publish(ctx, queue.Message{ID: messageID, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	processor := worker.New(broker, resultStore, validator.Basic{}, worker.WithMaxAttempts(3))
	if result, err := processor.ProcessOne(ctx); err != nil || !result.Inserted {
		t.Fatalf("process result=%+v error=%v", result, err)
	}
	exists, err := resultStore.Contains(ctx, messageID)
	if err != nil || !exists {
		t.Fatalf("expected durable result, exists=%v error=%v", exists, err)
	}
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
