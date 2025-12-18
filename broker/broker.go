package broker

import (
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/fr0stylo/line/core"
)

// Broker represents the message broker instance, combining the core queue
// logic with the gRPC server interface.
type Broker struct {
	queue  *core.Line
	server *grpc.Server
}

// NewBroker initializes a new Broker with the provided options.
// It sets up the underlying storage and prepares the gRPC server.
func NewBroker(opts ...Option) (*Broker, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	q, err := core.NewLine(cfg.store)
	if err != nil {
		return nil, err
	}

	srv := &queueServer{queue: q, ack: NewAckManager(cfg.ackOptions...)}

	grpcServer := grpc.NewServer(cfg.opts...)
	rpc.RegisterLineBrokerServer(grpcServer, srv)

	reflection.Register(grpcServer)

	return &Broker{
		queue:  q,
		server: grpcServer,
	}, nil
}

// Shutdown gracefully stops the gRPC server and closes the underlying queue storage.
func (b *Broker) Shutdown() error {
	b.server.GracefulStop()

	return b.queue.Close()
}

// ListenAndServe starts the gRPC server on the specified TCP address.
// This is a blocking call.
func (b *Broker) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return b.server.Serve(l)
}
