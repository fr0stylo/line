package line

import (
	"context"
	"log/slog"

	"line/store"
)

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Stream(context.Context) (<-chan []byte, error)
}

type Line struct {
	fs store.Store
}

func (l *Line) Close() error {
	return l.fs.Close()
}

func (l *Line) Push(blob []byte) error {
	if _, err := l.fs.Write(blob); err != nil {
		return err
	}

	return nil
}

func (l *Line) Pop() ([]byte, error) {
	return l.fs.Read()
}

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

			blob, err := l.fs.Read()
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

func NewLine(store store.Store) (*Line, error) {
	return &Line{
		fs: store,
	}, nil
}
