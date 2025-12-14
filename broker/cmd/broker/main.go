package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fr0stylo/line/broker"
	"github.com/fr0stylo/line/core/store"
)

const Megabyte = 1024 * 1024

func main() {
	var (
		addr        string
		dataDir     string
		segmentSize int64
		useMemory   bool
	)

	flag.StringVar(&addr, "addr", ":50051", "gRPC server address")
	flag.StringVar(&dataDir, "data-dir", "./data", "data directory for persistent storage")
	flag.Int64Var(
		&segmentSize,
		"segment-size",
		64*Megabyte,
		"segment file size in bytes (default 64MB)",
	)
	flag.BoolVar(&useMemory, "memory", false, "use in-memory storage (non-persistent)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Configure store
	var opts []broker.Option
	if !useMemory {
		s, err := store.NewSegmentStore(dataDir, uint64(segmentSize))
		if err != nil {
			slog.Error("Failed to create segment store", "error", err)
			os.Exit(1)
		}
		opts = append(opts, broker.WithStore(s))
		slog.Info("Using persistent storage", "dir", dataDir, "segment_size", segmentSize)
	} else {
		slog.Info("Using in-memory storage")
	}

	// Create broker
	b, err := broker.NewBroker(opts...)
	if err != nil {
		slog.Error("Failed to create broker", "error", err)
		os.Exit(1)
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		slog.Info("Starting gRPC server", "addr", addr)
		if err := b.ListenAndServe(addr); err != nil {
			errCh <- err
		}
	}()

	// Wait for a shutdown signal or error
	select {
	case <-ctx.Done():
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down", "signal", sig)
	case err := <-errCh:
		slog.Error("Server error", "error", err)
	}

	// Graceful shutdown
	slog.Info("Shutting down broker...")
	if err := b.Shutdown(); err != nil {
		slog.Error("Shutdown error", "error", err)
		os.Exit(1)
	}

	slog.Info("Broker stopped")
}

func init() {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		_, _ = fmt.Fprintln(os.Stderr, "Line Broker - A durable message queue server")
		_, _ = fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
		_, _ = fmt.Fprintln(os.Stderr, "\nExamples:")
		_, _ = fmt.Fprintln(
			os.Stderr,
			"  broker                          # Start with defaults (persistent, :50051)",
		)
		_, _ = fmt.Fprintln(os.Stderr, "  broker -addr :9090              # Custom port")
		_, _ = fmt.Fprintln(os.Stderr, "  broker -memory                  # In-memory mode")
		_, _ = fmt.Fprintln(os.Stderr, "  broker -data-dir /var/lib/line  # Custom data directory")
	}
}
