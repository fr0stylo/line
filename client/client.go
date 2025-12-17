package client

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
)

const telemetryTracerName = "line/client"

type Client struct {
	client rpc.LineBrokerClient
	conn   *grpc.ClientConn
	id     string
}

func NewClient(addr string, opts ...grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	client := rpc.NewLineBrokerClient(conn)
	id, _ := uuid.NewV7()

	return &Client{
		conn:   conn,
		client: client,
		id:     id.String(),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Publish(ctx context.Context, payload []byte) error {
	ctx, span := otel.Tracer(telemetryTracerName).Start(ctx, "Publish")
	defer span.End()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	id, _ := uuid.NewV7()
	_, err := c.client.Publish(ctx, &rpc.PublishRequest{Envelope: &envelope.Envelope{
		Payload:   payload,
		Id:        id.String(),
		Timestamp: time.Now().UnixMilli(),
		Baggage:   carrier,
	}})

	return err
}

func (c *Client) Handle(
	ctx context.Context,
	handler func(ctx context.Context, payload []byte) error,
) error {
	errChan := make(chan error, 1)
	stream, err := c.client.Subscribe(context.Background())
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer stream.CloseSend() //nolint:errcheck

	err = stream.Send(&rpc.SubscribeRequest{
		Type:         rpc.SubscribeType_INIT,
		SubscriberId: c.id,
	})
	if err != nil {
		slog.Error("failed to send subscribe request", "error", err.Error())
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := stream.Recv()
			if err != nil {
				slog.Error("failed to receive message", "error", err.Error())

				errChan <- err

				return
			}

			octx := otel.GetTextMapPropagator().
				Extract(ctx, propagation.MapCarrier(msg.GetBaggage()))

			err = handler(octx, msg.GetPayload())
			result := rpc.SubscribeType_ACK
			if err != nil {
				result = rpc.SubscribeType_NACK

				slog.Error("failed to handle message", "error", err.Error())
			}

			err = stream.Send(&rpc.SubscribeRequest{
				Type:         result,
				SubscriberId: c.id,
				MessageId:    msg.GetId(),
			})
			if err != nil {
				slog.Error("failed to send ack/nack", "error", err.Error())
				errChan <- err

				return
			}
		}
	}()

	return <-errChan
}
