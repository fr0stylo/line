package broker

import (
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/fr0stylo/line/core"
)

type Broker struct {
	queue  *core.Line
	server *grpc.Server
}

// NewBroker creates a Broker that wires an in‑memory core.Line to a gRPC server.
// It applies the provided Option functions to a default configuration, initializes
// the in-memory queue, registers the LineBroker RPC service on a new gRPC server,
// and enables server reflection. Returns the constructed Broker or an error if
// the queue initialization fails.
func NewBroker(opts ...Option) (*Broker, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(cfg)
	}

	q, err := core.NewLine(cfg.store)
	if err != nil {
		return nil, err
	}

	srv := &server{queue: q}

	grpcServer := grpc.NewServer(cfg.opts...)
	rpc.RegisterLineBrokerServer(grpcServer, srv)

	reflection.Register(grpcServer)

	return &Broker{
		queue:  q,
		server: grpcServer,
	}, nil
}

func (b *Broker) Shutdown() error {
	b.server.GracefulStop()

	return b.queue.Close()
}

func (b *Broker) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return b.server.Serve(l)
}