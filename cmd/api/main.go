package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ajaykr0905/distributed-scale-validation-lab/internal/controlapi"
)

func main() {
	address := os.Getenv("SCALE_LAB_ADDRESS")
	if address == "" {
		address = ":8080"
	}
	server := &http.Server{
		Addr:              address,
		Handler:           controlapi.New(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("scale validation control API listening on %s", address)
	log.Fatal(server.ListenAndServe())
}
