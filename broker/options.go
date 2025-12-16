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

// defaultOptions creates an *options with default settings: an in-memory store and an empty slice of gRPC server options.
func defaultOptions() *options {
	return &options{
		store: store.NewMemory(),
		opts:  []grpc.ServerOption{},
	}
}

// WithStore returns an Option that sets the broker's queue store to the provided store implementation.
// The returned Option replaces the default in-memory store when applied.
func WithStore(s store.Store) Option {
	return func(o *options) {
		o.store = s
	}
}

// WithGrpcOptions returns an Option that sets the gRPC server options used by the broker
// to the provided options, replacing any existing gRPC options.
func WithGrpcOptions(opts ...grpc.ServerOption) Option {
	return func(o *options) {
		o.opts = opts
	}
}