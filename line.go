package line

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/fr0stylo/line/gen/envelope"
	"github.com/fr0stylo/line/store"
)

// Queue describes a durable FIFO that supports discrete push/pop semantics and
// a streaming consumer API.
type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Stream(context.Context) (<-chan []byte, error)
}

// Line exposes a Queue implementation backed by a store.Store.
type Line struct {
	fs store.Store
}

// Close flushes metadata and releases resources held by the underlying store.
func (l *Line) Close() error {
	return l.fs.Close()
}

// Push enqueues a new message into the durable store.
func (l *Line) Push(blob []byte) error {
	payload := envelope.Envelope{
		Id:         "",
		Timestamp:  0,
		Baggage:    nil,
		Attributes: nil,
		Payload:    blob,
	}

	buf, err := proto.Marshal(&payload)
	if err != nil {
		return err
	}

	if _, err := l.fs.Write(buf); err != nil {
		return err
	}

	return nil
}

// Pop blocks until the next message is available and returns it.
func (l *Line) Pop() ([]byte, error) {
	buf, err := l.fs.Read()
	if err != nil {
		return nil, err
	}

	var payload envelope.Envelope
	if err := proto.Unmarshal(buf, &payload); err != nil {
		return nil, err
	}

	return payload.GetPayload(), nil
}

// Stream continuously emits messages until the context is cancelled or the
// store read fails. The returned channel is closed on exit.
func (l *Line) Stream(ctx context.Context) <-chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:

			}

			blob, err := l.Pop()
			if err != nil {
				slog.Error("Failed to read message", "error", err)
				continue
			}

			select {
			case ch <- blob:

			case <-ctx.Done():
				return
			}

		}
	}()

	return ch
}

// NewLine wires a Line on top of the provided store implementation.
func NewLine(store store.Store) (*Line, error) {
	return &Line{
		fs: store,
	}, nil
}
