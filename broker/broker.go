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
