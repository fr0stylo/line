package client

import (
	"context"
	"log/slog"
	"time"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	client rpc.LineBrokerClient
	conn   *grpc.ClientConn
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := rpc.NewLineBrokerClient(conn)
	return &Client{
		conn:   conn,
		client: client,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Publish(ctx context.Context, payload []byte) error {
	_, err := c.client.Publish(ctx, &rpc.PublishRequest{Envelope: &envelope.Envelope{
		Payload:   payload,
		Id:        uuid.New().String(),
		Timestamp: time.Now().UnixMilli(),
	}})

	return err
}

func (c *Client) Handle(handler func(ctx context.Context, payload []byte) error) error {
	errChan := make(chan error, 1)
	stream, err := c.client.Subscribe(context.Background(), &rpc.SubscribeRequest{})
	if err != nil {
		return err
	}

	go func() {
		defer stream.CloseSend() //nolint:errcheck
		for {
			msg, err := stream.Recv()
			if err != nil {
				slog.Error("failed to receive message", "error", err.Error())
				errChan <- err
				return
			}

			ctx := context.Background()
			if err := handler(ctx, msg.GetPayload()); err != nil {
				slog.Error("failed to handle message", "error", err.Error())
				errChan <- err
				return
			}
		}
	}()

	return <-errChan
}
