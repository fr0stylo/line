package broker

import (
	"context"
	"fmt"
	"log"

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
	if err := s.queue.PushContext(ctx, env.GetPayload()); err != nil {
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

		_, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive subscribe request: %w", err)
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
