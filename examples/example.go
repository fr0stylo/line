package main

import (
	"context"
	"log/slog"
	"time"

	"line"
	"line/store"
)

func main() {
	s, err := store.NewSegmentStore("./dir/", 1024)
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer s.Close() //nolint:errcheck // best effort cleanup

	q, err := line.NewLine(s)
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer q.Close() //nolint:errcheck // best effort cleanup
	slog.Info("Line queue created successfully")

	go func() {
		for range time.NewTicker(100 * time.Millisecond).C {
			if err := q.Push([]byte("message from " + time.Now().Format(time.RFC3339))); err != nil {
				slog.Error("Failed to enqueue message", "error", err)
			}
		}
	}()
	go func() {
		for range time.NewTicker(4 * time.Second).C {
			if err := q.Push([]byte("message 2 from " + time.Now().Format(time.RFC3339))); err != nil {
				slog.Error("Failed to enqueue message", "error", err)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for msg := range q.Stream(ctx) {
		slog.Info(string(msg))
	}
}
