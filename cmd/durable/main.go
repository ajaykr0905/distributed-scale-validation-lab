// durable runs independent submission, outbox, and worker processes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/durable"
	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/validator"
)

func main() {
	role := flag.String("role", "api", "api, dispatcher, or worker")
	address := flag.String("address", "127.0.0.1:8081", "API listening address")
	queueName := flag.String("queue", "scale-lab-durable", "isolated durable queue name")
	capacity := flag.Int("capacity", 100000, "maximum unfinished accepted jobs")
	maxAttempts := flag.Int("max-attempts", 3, "validation attempt budget (1..10)")
	retryDelay := flag.Duration("retry-delay", time.Second, "delay before validation retry")
	flag.Parse()
	if *role != "api" && *role != "dispatcher" && *role != "worker" {
		log.Fatal("invalid role")
	}
	if *capacity < 1 || *maxAttempts < 1 || *maxAttempts > 10 || *retryDelay < 0 || *retryDelay > time.Minute {
		log.Fatal("invalid bounded controls")
	}
	postgresURL := os.Getenv("SCALE_LAB_POSTGRES_URL")
	if postgresURL == "" {
		log.Fatal("SCALE_LAB_POSTGRES_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	db, err := durable.Open(connectCtx, postgresURL)
	if err == nil {
		err = db.EnsureSchema(connectCtx)
	}
	cancel()
	if err != nil {
		log.Fatal("PostgreSQL initialization failed: ", err)
	}
	defer db.Close()
	if *role == "api" {
		if err := serve(ctx, *address, durable.HTTP(db, *maxAttempts, *capacity)); err != nil {
			log.Fatal(err)
		}
		return
	}
	rabbitURL := os.Getenv("SCALE_LAB_RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("SCALE_LAB_RABBITMQ_URL is required")
	}
	for ctx.Err() == nil {
		connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		broker, err := durable.Connect(connectCtx, rabbitURL, *queueName, *role == "worker")
		cancel()
		if err == nil {
			log.Printf("%s connected; queue=%s", *role, *queueName)
			err = run(ctx, *role, db, broker, *retryDelay)
			_ = broker.Close()
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("%s reconnecting after failure: %v", *role, err)
		pause(ctx, time.Second)
	}
}

func serve(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	result := make(chan error, 1)
	go func() { result <- server.ListenAndServe() }()
	log.Printf("durable submission API listening on %s", address)
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}

func run(ctx context.Context, role string, db *durable.DB, broker *durable.Broker, retryDelay time.Duration) error {
	for ctx.Err() == nil {
		if role == "dispatcher" {
			step, cancel := context.WithTimeout(ctx, 10*time.Second)
			sent, err := db.DispatchOne(step, broker, durable.Hooks{})
			cancel()
			if err != nil {
				return err
			}
			if !sent {
				pause(ctx, 25*time.Millisecond)
			}
			continue
		}
		delivery, err := broker.Receive(ctx)
		if err != nil {
			return err
		}
		var event durable.Event
		decoder := json.NewDecoder(bytes.NewReader(delivery.Body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			if err := delivery.Nack(false, false); err != nil {
				return err
			}
			continue
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			if err := delivery.Nack(false, false); err != nil {
				return err
			}
			continue
		}
		step, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = db.Process(step, event, validator.Basic{}, retryDelay, durable.Hooks{})
		cancel()
		if err == nil {
			if err := delivery.Ack(false); err != nil {
				return err
			}
		} else {
			requeue := !errors.Is(err, durable.ErrNotFound) && !errors.Is(err, durable.ErrInvalidEvent)
			if nackErr := delivery.Nack(false, requeue); nackErr != nil {
				return nackErr
			}
			log.Printf("worker processing failed; requeue=%t: %v", requeue, err)
			if requeue {
				pause(ctx, 250*time.Millisecond)
			}
		}
	}
	return ctx.Err()
}

func pause(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
