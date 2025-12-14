package broker

import (
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/fr0stylo/line/contracts/gen/rpc"
	"github.com/fr0stylo/line/core"
	"github.com/fr0stylo/line/core/store"
)

type Option func(*options)

type options struct {
	store store.Store
}

func defaultOptions() *options {
	return &options{
		store: store.NewMemory(),
	}
}

// WithStore sets a custom store implementation for the broker's queue.
func WithStore(s store.Store) Option {
	return func(o *options) {
		o.store = s
	}
}

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

	grpcServer := grpc.NewServer()
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
