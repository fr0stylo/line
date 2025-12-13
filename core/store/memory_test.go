package store

import (
	"io"
	"testing"
	"time"
)

func TestMemoryWriteReadOrder(t *testing.T) {
	ms := NewMemory()

	payloads := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for _, p := range payloads {
		if _, err := ms.Write(p); err != nil {
			t.Fatalf("write %q: %v", p, err)
		}
	}

	for i, want := range payloads {
		got, err := ms.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("expected %q, got %q", want, got)
		}
	}
}

func TestMemoryBlocksUntilWrite(t *testing.T) {
	ms := NewMemory()

	errCh := make(chan error, 1)
	msgCh := make(chan []byte, 1)
	go func() {
		msg, err := ms.Read()
		msgCh <- msg
		errCh <- err
	}()

	select {
	case <-msgCh:
		t.Fatalf("read returned before write")
	case <-time.After(50 * time.Millisecond):
	}

	want := []byte("delayed")
	if _, err := ms.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		payload := <-msgCh
		if string(payload) != string(want) {
			t.Fatalf("expected %q, got %q", want, payload)
		}
	case <-time.After(time.Second):
		t.Fatalf("read did not unblock after write")
	}
}

func TestMemoryCloseUnblocksRead(t *testing.T) {
	ms := NewMemory()

	errCh := make(chan error, 1)
	go func() {
		_, err := ms.Read()
		errCh <- err
	}()

	select {
	case <-errCh:
		t.Fatalf("read returned without close")
	case <-time.After(50 * time.Millisecond):
	}

	if err := ms.Close(); err != nil {
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
