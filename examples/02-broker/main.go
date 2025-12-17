package main

import (
	"context"
	"flag"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/fr0stylo/line/broker"
	"github.com/fr0stylo/line/client"
)

var wg sync.WaitGroup

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
		go StartClientSubscriber(ctx, i+1)
	}

	wg.Wait()
}

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

func StartClientPublisher(ctx context.Context, id any, i time.Duration) {
	defer wg.Done()

	c, err := client.NewClient(":8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func StartClientSubscriber(ctx context.Context, id any) {
	defer wg.Done()

	c, err := client.NewClient(":8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Failed to create client", "error", err)

		return
	}
	defer c.Close() //nolint:errcheck

	if err := c.Handle(ctx, func(ctx context.Context, msg []byte) error {
		dur := time.Duration(1000*rand.Float64()) * time.Millisecond
		slog.Info("Received message", "receiver", id, "message", string(msg), "sleep", dur.String())
		time.Sleep(dur)

		return nil
	}); err != nil {
		slog.Error("Failed to handle message", "error", err)

		return
	}
}
