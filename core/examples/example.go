package main

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"

	line "github.com/fr0stylo/line/core"
	"github.com/fr0stylo/line/core/store"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// initTracer configures a basic OpenTelemetry tracer provider with an OTLP/gRPC exporter
// so that spans from the example (and the library) are exported over gRPC.
func initTracer(ctx context.Context) (func(context.Context) error, error) {
	// Configure OTLP gRPC exporter (endpoint default: localhost:4317)
	//endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	//if endpoint == "" {
	//	endpoint = "localhost:4317"
	//}
	//insecure := true
	//if v := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); v != "" {
	//	insecure = v == "1" || v == "true" || v == "TRUE" || v == "True"
	//}
	//
	//clientOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	//if insecure {
	//	clientOpts = append(clientOpts, otlptracegrpc.WithInsecure())
	//}

	exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient())
	if err != nil {
		return nil, err
	}

	// Define a resource describing this service
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", "line-example"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())

	mprov := sdktmetric.NewMeterProvider(sdktmetric.WithResource(res), sdktmetric.WithReader(sdktmetric.NewPeriodicReader(metricExporter)))
	otel.SetMeterProvider(mprov)

	die := func(ctx context.Context) error {
		if err := mprov.Shutdown(ctx); err != nil {
			return err
		}
		return tp.Shutdown(ctx)
	}

	return die, nil
}

func main() {
	// Set up OpenTelemetry tracing for the example.
	shutdown, err := initTracer(context.Background())
	if err != nil {
		slog.Error("failed to initialize tracing", "error", err)
	} else {
		defer func() {
			// best-effort shutdown with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdown(ctx)
		}()
	}

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
			ctx, span := otel.Tracer("example").Start(context.Background(), "fast")
			if err := q.PushContext(ctx, []byte("message from "+time.Now().Format(time.RFC3339))); err != nil {
				slog.Error("Failed to enqueue message", "error", err)
			}
			span.End()
		}
	}()
	go func() {
		for range time.NewTicker(4 * time.Second).C {
			ctx, span := otel.Tracer("example").Start(context.Background(), "slow")
			if err := q.PushContext(ctx, []byte("message 2 from "+time.Now().Format(time.RFC3339))); err != nil {
				slog.Error("Failed to enqueue message", "error", err)
			}
			span.End()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()
	for msg := range q.Stream(ctx) {
		slog.Info(string(msg))
	}
}
