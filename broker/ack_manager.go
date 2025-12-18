package broker

import (
	"log/slog"
	"sync"
)

type ackOptions struct {
	MaxRequeueLength int
}

func defaultAckOptions() ackOptions {
	return ackOptions{
		MaxRequeueLength: 1024,
	}
}

// AckOptions is a function type for configuring the AckManager.
type AckOptions func(*ackOptions)

// WithMaxRequeueLength sets the maximum size of the internal requeue buffer.
func WithMaxRequeueLength(length int) AckOptions {
	return func(opts *ackOptions) {
		opts.MaxRequeueLength = length
	}
}

// AckManager handles the lifecycle of message acknowledgments, tracking
// in-flight messages and managing those that need to be requeued.
type AckManager struct {
	ackMap  sync.Map
	requeue chan any
}

// NewAckManager creates a new instance of AckManager with the provided options.
func NewAckManager(opts ...AckOptions) *AckManager {
	defaultOpts := defaultAckOptions()
	for _, opt := range opts {
		opt(&defaultOpts)
	}

	return &AckManager{
		ackMap:  sync.Map{},
		requeue: make(chan any, defaultOpts.MaxRequeueLength),
	}
}

// Ack removes a message from the tracking map, marking it as successfully processed.
func (a *AckManager) Ack(id string) {
	a.ackMap.Delete(id)
}

// Nack removes a message from the tracking map and places it back into
// the requeue channel for redelivery.
func (a *AckManager) Nack(id string) {
	val, ok := a.ackMap.LoadAndDelete(id)
	if ok {
		slog.Warn("Nacked message", "id", id, "message", val)
		a.requeue <- val
	}
}

// Pop attempts to retrieve a previously NACKed message from the requeue buffer.
// It returns nil immediately if no messages are waiting to be redelivered.
func (a *AckManager) Pop() any {
	select {
	case val := <-a.requeue:
		slog.Info("Popped from requeue")

		return val
	default:
		slog.Info("No messages in requeue")

		return nil
	}
}

// Initiate begins tracking a message that has been sent to a subscriber.
func (a *AckManager) Initiate(id string, msg any) {
	a.ackMap.Store(id, msg)
}
