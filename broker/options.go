package broker

import (
	"google.golang.org/grpc"

	"github.com/fr0stylo/line/core/store"
)

// Option is a function type for configuring the Broker.
type Option func(*options)

type options struct {
	store      store.Store
	opts       []grpc.ServerOption
	ackOptions []AckOptions
}

func defaultOptions() *options {
	return &options{
		store:      store.NewMemory(),
		opts:       []grpc.ServerOption{},
		ackOptions: []AckOptions{WithMaxRequeueLength(1024)},
	}
}

// WithAckOptions passes configuration options to the internal AckManager.
func WithAckOptions(opts ...AckOptions) Option {
	return func(o *options) {
		o.ackOptions = opts
	}
}

// WithStore sets a custom store implementation for the broker's queue.
func WithStore(s store.Store) Option {
	return func(o *options) {
		o.store = s
	}
}

// WithGrpcOptions adds custom gRPC server options to the broker's server.
func WithGrpcOptions(opts ...grpc.ServerOption) Option {
	return func(o *options) {
		o.opts = opts
	}
}
