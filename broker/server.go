package broker

import (
	"context"
	"fmt"
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

	ack   *AckManager
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
			slog.Info("New subscriber initialized", "subscriber_id", req.GetSubscriberId())
		case rpc.SubscribeType_ACK:
			slog.Info(
				"Subscriber acknowledged message",
				"subscriber_id",
				req.GetSubscriberId(),
				"message_id",
				req.GetMessageId(),
			)
			s.ack.Ack(req.GetMessageId())
		case rpc.SubscribeType_NACK:
			slog.Info(
				"Subscriber rejected message",
				"subscriber_id",
				req.GetSubscriberId(),
				"message_id",
				req.GetMessageId(),
			)
			s.ack.Nack(req.GetMessageId())
		default:
		}

		msg := s.ack.Pop()
		if msg == nil {
			slog.Info("No messages to send")
			msg, err = s.queue.Pop()
			slog.Info("Popped message", "message_id", msg.(*envelope.Envelope).GetId())
			if err != nil {
				return fmt.Errorf("failed to pop message: %w", err)
			}
		}

		slog.Info("Sending message", "message_id", msg.(*envelope.Envelope).GetId())
		err = stream.SendMsg(msg)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
		slog.Info("Message sent")
		s.ack.Initiate(msg.(*envelope.Envelope).GetId(), msg)
	}
}
