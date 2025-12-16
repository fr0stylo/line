package main

import (
	"context"
	"flag"
	"log/slog"
	"sync"
	"time"

	"github.com/fr0stylo/line/broker"
	"github.com/fr0stylo/line/client"
)

var wg sync.WaitGroup

// main parses command-line flags for producers, consumers, and publish intervals, starts the broker server,
// launches the configured publisher and subscriber goroutines, and waits for all of them to finish.
func main() {
	p := flag.Int("p", 1, "Number of producers")
	c := flag.Int("c", 1, "Number of consumers")
	id := flag.Duration("i", 1*time.Second, "Publish interval")
	ir := flag.Int("ir", 10, "Publish interval randomization")
	flag.Parse()

	ctx := context.Background()

	wg.Add(1)
	go StartServer()

	for i := 0; i < *p; i++ {
		wg.Add(1)
		go StartClientPublisher(ctx, 1, *id*time.Duration(1+*ir))
	}
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go StartClientSubscriber(i + 1)
	}

	wg.Wait()
}

// StartServer creates a broker, starts listening on ":8080", and blocks until the server stops.
// It decrements the package-level WaitGroup when it returns. On broker creation or listen failure
// it logs a descriptive error message.
func StartServer() {
	defer wg.Done()

	b, err := broker.NewBroker()
	if err != nil {
		slog.Error("Failed to create broker", "error", err)
		return
	}

	if err := b.ListenAndServe(":8080"); err != nil {
		slog.Error("Failed to start broker", "error", err)

		return
	}
}

// StartClientPublisher connects to the broker and publishes "Hello, World!" at the specified interval.
// 
// It uses ctx for publish operations and creates a client connected to :8080. The publisher runs until
// client creation or a publish operation fails, logging errors before returning. The id parameter is an
// arbitrary identifier for the caller and is not interpreted by this function. i is the interval between publishes.
func StartClientPublisher(ctx context.Context, id any, i time.Duration) {
	defer wg.Done()

	c, err := client.NewClient(":8080")
	if err != nil {
		slog.Error("Failed to create client", "error", err)
		return
	}
	defer c.Close() //nolint:errcheck

	ticker := time.NewTicker(i)

	for range ticker.C {
		if err := c.Publish(ctx, []byte("Hello, World!")); err != nil {
			slog.Error("Failed to publish message", "error", err)
			return
		}
	}
}

// StartClientSubscriber creates a client connected to ":8080" and registers a handler that logs each received message with the provided subscriber id.
// The id parameter is included in log entries to identify which subscriber received a message.
func StartClientSubscriber(id any) {
	defer wg.Done()

	c, err := client.NewClient(":8080")
	if err != nil {
		slog.Error("Failed to create client", "error", err)
		return
	}
	defer c.Close() //nolint:errcheck

	if err := c.Handle(func(ctx context.Context, msg []byte) error {
		slog.Info("Received message", "receiver", id, "message", string(msg))

		return nil
	}); err != nil {
		slog.Error("Failed to handle message", "error", err)
		return
	}
}