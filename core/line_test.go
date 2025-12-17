package core

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	"github.com/fr0stylo/line/core/store"
)

func TestLinePushPop(t *testing.T) {
	store := store.NewMemory()
	q, err := NewLine(store)
	if err != nil {
		t.Fatalf("new line: %v", err)
	}

	want := []byte("hello")
	if err := q.Push(newEnvelope(want)); err != nil {
		t.Fatalf("push: %v", err)
	}

	got, err := q.Pop()
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if got == nil {
		t.Fatalf("pop returned nil envelope")
	}
	if !bytes.Equal(got.GetPayload(), want) {
		t.Fatalf("expected %q, got %q", want, got.GetPayload())
	}
}

func TestLineStreamCancels(t *testing.T) {
	store := store.NewMemory()
	q, err := NewLine(store)
	if err != nil {
		t.Fatalf("new line: %v", err)
	}

	messages := [][]byte{[]byte("one"), []byte("two")}
	for _, msg := range messages {
		if err := q.Push(newEnvelope(msg)); err != nil {
			t.Fatalf("push %q: %v", msg, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stream := q.Stream(ctx)

	for i, want := range messages {
		select {
		case got, ok := <-stream:
			if !ok {
				t.Fatalf("stream closed early at index %d", i)
			}
			if got == nil {
				t.Fatalf("stream returned nil envelope at index %d", i)
			}
			if !bytes.Equal(got.GetPayload(), want) {
				t.Fatalf("expected %q, got %q", want, got.GetPayload())
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}

	cancel()

	select {
	case _, ok := <-stream:
		if ok {
			t.Fatalf("expected stream to close after cancel")
		}
	case <-time.After(time.Second):
		t.Fatalf("stream did not close after cancel")
	}
}

func TestMemoryStoreWriteReadOrder(t *testing.T) {
	store := store.NewMemory()

	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, p := range payloads {
		if _, err := store.Write(p); err != nil {
			t.Fatalf("write %q: %v", p, err)
		}
	}

	for i, want := range payloads {
		got, err := store.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("expected %q, got %q", want, got)
		}
	}
}

func TestMemoryStoreCloseUnblocksRead(t *testing.T) {
	store := store.NewMemory()

	errCh := make(chan error, 1)
	go func() {
		_, err := store.Read()
		errCh <- err
	}()

	select {
	case <-errCh:
		t.Fatalf("read returned without close")
	case <-time.After(50 * time.Millisecond):
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-errCh:
		if err != io.EOF {
			t.Fatalf("expected EOF after close, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("read did not unblock after close")
	}
}

func newEnvelope(payload []byte) *envelope.Envelope {
	return &envelope.Envelope{
		Payload: append([]byte(nil), payload...),
	}
}
