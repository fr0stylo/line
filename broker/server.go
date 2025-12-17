package broker

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/fr0stylo/line/core"
)

type queueServer struct {
	rpc.UnimplementedLineBrokerServer

	queue *core.Line
}

func (s *queueServer) Publish(
	ctx context.Context,
	req *rpc.PublishRequest,
) (*rpc.PublishResponse, error) {
	env := req.GetEnvelope()
	octx := otel.GetTextMapPropagator().
		Extract(ctx, propagation.MapCarrier(env.GetBaggage()))

	if err := s.queue.PushContext(octx, env); err != nil {
		return nil, err
	}

	return &rpc.PublishResponse{
		Success: true,
		Message: "Message published successfully",
	}, nil
}

func (s *queueServer) Subscribe(
	stream grpc.BidiStreamingServer[rpc.SubscribeRequest, envelope.Envelope],
) error {
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive subscribe request: %w", err)
		}

		switch req.GetType() {
		case rpc.SubscribeType_INIT:
			slog.Debug("New subscriber initialized", "subscriber_id", req.GetSubscriberId())
		case rpc.SubscribeType_ACK:
			slog.Debug(
				"Subscriber acknowledged message",
				"subscriber_id",
				req.GetSubscriberId(),
				"message_id",
				req.GetMessageId(),
			)
		case rpc.SubscribeType_NACK:
			slog.Debug(
				"Subscriber rejected message",
				"subscriber_id",
				req.GetSubscriberId(),
				"message_id",
				req.GetMessageId(),
			)
		default:
		}

		msg, err := s.queue.Pop()
		if err != nil {
			return fmt.Errorf("failed to pop message: %w", err)
		}

		err = stream.SendMsg(msg)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
	}
}

func (s *queueServer) Acknowledge(
	_ context.Context,
	req *rpc.AcknowledgeRequest,
) (*rpc.AcknowledgeResponse, error) {
	log.Printf("Acknowledge called: id=%s", req.GetMessageId())

	return &rpc.AcknowledgeResponse{}, nil
}
