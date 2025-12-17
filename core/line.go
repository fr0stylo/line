package core

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/core/store"
)

const (
	telemetryTracerName = "line"
)

var (
	metricsOnce sync.Once
	pushCounter metric.Int64Counter
	popCounter  metric.Int64Counter
)

// Queue describes a durable FIFO that supports discrete push/pop semantics and
// a streaming consumer API.
type Queue interface {
	Push(payload *envelope.Envelope) error
	PushContext(ctx context.Context, payload *envelope.Envelope) error
	Pop() (*envelope.Envelope, error)
	PopContext(ctx context.Context) (*envelope.Envelope, error)
	Stream(ctx context.Context) (<-chan *envelope.Envelope, error)
}

// Line exposes a Queue implementation backed by a store.Store.
type Line struct {
	fs store.Store
}

// NewLine wires a Line on top of the provided store implementation.
func NewLine(store store.Store) (*Line, error) {
	return &Line{
		fs: store,
	}, nil
}

// Close flushes metadata and releases resources held by the underlying store.
func (l *Line) Close() error {
	return l.fs.Close()
}

// Push enqueues a new message into the durable store.
func (l *Line) Push(blob *envelope.Envelope) error {
	return l.PushContext(context.Background(), blob)
}

// PushContext enqueues a new message into the durable store, propagating tracing
// information from the provided context.
func (l *Line) PushContext(ctx context.Context, blob *envelope.Envelope) error {
	initMetrics()

	ctx, span := otel.Tracer(telemetryTracerName).Start(ctx, "Push")
	defer span.End()

	if blob.Baggage == nil {
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)

		blob.Baggage = carrier
	}

	blob.Timestamp = time.Now().UnixMilli()

	buf, err := proto.Marshal(blob)
	if err != nil {
		recordPush(ctx, "marshal_error")

		return err
	}

	if _, err := l.fs.Write(buf); err != nil {
		recordPush(ctx, "store_error")

		return err
	}

	recordPush(ctx, "success")

	return nil
}

// Pop blocks until the next message is available and returns it.
func (l *Line) Pop() (*envelope.Envelope, error) {
	initMetrics()

	ctx := context.Background()

	buf, err := l.fs.Read()
	if err != nil {
		recordPop(ctx, "store_error")

		return nil, err
	}

	var payload envelope.Envelope

	err = proto.Unmarshal(buf, &payload)
	if err != nil {
		recordPop(ctx, "decode_error")

		return nil, fmt.Errorf("failed to decode payload: %w", err)
	}

	carrier := propagation.MapCarrier(payload.GetBaggage())
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	_, span := otel.Tracer(telemetryTracerName).Start(ctx, "Pop")
	defer span.End()

	recordPop(ctx, "success")

	return &payload, nil
}

// Stream continuously emits messages until the context is cancelled or the
// store read fails. The returned channel is closed on exit.
func (l *Line) Stream(ctx context.Context) <-chan *envelope.Envelope {
	envelopes := make(chan *envelope.Envelope)

	go func() {
		defer close(envelopes)

		for {
			select {
			case <-ctx.Done():
				return
			default:

			}

			blob, err := l.Pop()
			if err != nil {
				slog.Error("Failed to read message", "error", err)

				continue
			}

			select {
			case envelopes <- blob:

			case <-ctx.Done():
				return
			}

		}
	}()

	return envelopes
}

func initMetrics() {
	metricsOnce.Do(func() {
		meter := otel.Meter(telemetryTracerName)

		var err error

		pushCounter, err = meter.Int64Counter(
			"line.push.count",
			metric.WithDescription("Number of push attempts"),
		)
		if err != nil {
			slog.Error("Failed to create push counter", "error", err)
		}

		popCounter, err = meter.Int64Counter(
			"line.pop.count",
			metric.WithDescription("Number of pop attempts"),
		)
		if err != nil {
			slog.Error("Failed to create pop counter", "error", err)
		}
	})
}

func recordPush(ctx context.Context, result string) {
	if pushCounter == nil {
		return
	}

	pushCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func recordPop(ctx context.Context, result string) {
	if popCounter == nil {
		return
	}

	popCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}
