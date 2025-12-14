package broker

import (
	"context"
	"log"

	"google.golang.org/grpc"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/fr0stylo/line/core"
)

type server struct {
	rpc.UnimplementedLineBrokerServer
	queue *core.Line
}

func (s *server) Publish(
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

func (s *server) Subscribe(
	req *rpc.SubscribeRequest,
	stream grpc.ServerStreamingServer[envelope.Envelope],
) error {
	msgStream := s.queue.Stream(stream.Context())
	for msg := range msgStream {
		env := &envelope.Envelope{
			Payload: msg,
		}
		if err := stream.Send(env); err != nil {
			log.Printf("Failed to send message: %v", err)

			return err
		}
	}

	return nil
}

func (s *server) Acknowledge(
	ctx context.Context,
	req *rpc.AcknowledgeRequest,
) (*rpc.AcknowledgeResponse, error) {
	log.Printf("Acknowledge called: id=%s", req.GetMessageId())

	return &rpc.AcknowledgeResponse{}, nil
}
