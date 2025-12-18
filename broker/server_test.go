package broker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/fr0stylo/line/core"
	"github.com/fr0stylo/line/core/store"
)

func TestQueueServer_Publish(t *testing.T) {
	// Setup dependencies
	memStore := store.NewMemory()
	line, _ := core.NewLine(memStore)
	server := &queueServer{
		queue: line,
		ack:   NewAckManager(),
	}

	ctx := context.Background()
	req := &rpc.PublishRequest{
		Envelope: &envelope.Envelope{
			Id:      "msg-1",
			Payload: []byte("test data"),
		},
	}

	// Execute
	resp, err := server.Publish(ctx, req)

	// Assert
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "Message published successfully", resp.Message)

	// Verify it reached the queue
	popped, _ := line.Pop()
	assert.Equal(t, "msg-1", popped.GetId())
}

// Mock for gRPC Bidi stream
type mockSubscribeStream struct {
	grpc.BidiStreamingServer[rpc.SubscribeRequest, envelope.Envelope]
	mock.Mock
}

func (m *mockSubscribeStream) Context() context.Context {
	return m.Called().Get(0).(context.Context)
}

func (m *mockSubscribeStream) Recv() (*rpc.SubscribeRequest, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rpc.SubscribeRequest), args.Error(1)
}

func (m *mockSubscribeStream) SendMsg(msg any) error {
	return m.Called(msg).Error(0)
}

func TestQueueServer_Subscribe_Init(t *testing.T) {
	memStore := store.NewMemory()
	line, _ := core.NewLine(memStore)
	server := &queueServer{
		queue: line,
		ack:   NewAckManager(),
	}

	// Prepare data in queue
	err := line.Push(&envelope.Envelope{Id: "1", Payload: []byte("m1")})
	require.NoError(t, err)

	mockStream := new(mockSubscribeStream)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream.On("Context").Return(ctx)

	// First call to Recv: INIT
	mockStream.On("Recv").Return(&rpc.SubscribeRequest{
		Type:         rpc.SubscribeType_INIT,
		SubscriberId: "sub1",
	}, nil).Once()

	// Capture the message sent to client
	mockStream.On("SendMsg", mock.MatchedBy(func(e *envelope.Envelope) bool {
		return e.Id == "1"
	})).Return(nil).Once()

	// Second call to Recv: trigger exit
	mockStream.On("Recv").Run(func(args mock.Arguments) {
		cancel()
	}).Return(nil, context.Canceled)

	// Run Subscribe
	err = server.Subscribe(mockStream)
	assert.ErrorIs(t, err, context.Canceled)
	mockStream.AssertExpectations(t)
}

func TestQueueServer_Subscribe_Nack(t *testing.T) {
	memStore := store.NewMemory()
	line, _ := core.NewLine(memStore)
	server := &queueServer{
		queue: line,
		ack:   NewAckManager(),
	}

	mockStream := new(mockSubscribeStream)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream.On("Context").Return(ctx)

	// 1. Send a message to be nacked later
	msg := &envelope.Envelope{Id: "retry-me", Payload: []byte("data")}
	server.ack.Initiate(msg.Id, msg)

	// Call Recv: NACK for that message
	mockStream.On("Recv").Return(&rpc.SubscribeRequest{
		Type:      rpc.SubscribeType_NACK,
		MessageId: "retry-me",
	}, nil).Once()

	// Expect SendMsg to receive the SAME message because it was nacked/requeued
	mockStream.On("SendMsg", mock.MatchedBy(func(e *envelope.Envelope) bool {
		return e.Id == "retry-me"
	})).Return(nil).Once()

	// Exit loop
	mockStream.On("Recv").Run(func(args mock.Arguments) {
		cancel()
	}).Return(nil, context.Canceled)

	err := server.Subscribe(mockStream)
	assert.ErrorIs(t, err, context.Canceled)
	mockStream.AssertExpectations(t)
}
