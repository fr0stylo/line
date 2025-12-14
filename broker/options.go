package broker

import (
	"google.golang.org/grpc"

	"github.com/fr0stylo/line/core/store"
)

type Option func(*options)

type options struct {
	store store.Store
	opts  []grpc.ServerOption
}

func defaultOptions() *options {
	return &options{
		store: store.NewMemory(),
		opts:  []grpc.ServerOption{},
	}
}

// WithStore sets a custom store implementation for the broker's queue.
func WithStore(s store.Store) Option {
	return func(o *options) {
		o.store = s
	}
}

func WithGrpcOptions(opts ...grpc.ServerOption) Option {
	return func(o *options) {
		o.opts = opts
	}
}
