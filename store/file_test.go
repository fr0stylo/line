package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreWriteReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	fs, err := NewFile(path)
	if err != nil {
		t.Fatalf("create file store: %v", err)
	}
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Fatalf("close file store: %v", err)
		}
	})

	payload := []byte("hello world")
	if _, err := fs.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	blob, err := fs.Read()
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(blob) != string(payload) {
		t.Fatalf("unexpected payload %q", blob)
	}
}

func TestFileStoreMetadataPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	fs, err := NewFile(path)
	if err != nil {
		t.Fatalf("create file store: %v", err)
	}

	if _, err := fs.Write([]byte("first")); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if _, err := fs.Write([]byte("second")); err != nil {
		t.Fatalf("write second: %v", err)
	}

	first, err := fs.Read()
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if string(first) != "first" {
		t.Fatalf("expected first, got %q", first)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("close initial store: %v", err)
	}

	reopened, err := NewFile(path)
	if err != nil {
		t.Fatalf("reopen file store: %v", err)
	}
	defer reopened.Close() //nolint:errcheck // best effort cleanup

	second, err := reopened.Read()
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if string(second) != "second" {
		t.Fatalf("expected second, got %q", second)
	}
}

func TestFileStoreReadBlocksUntilWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	fs, err := NewFile(path)
	if err != nil {
		t.Fatalf("create file store: %v", err)
	}
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Fatalf("close file store: %v", err)
		}
	})

	resultCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		payload, err := fs.Read()
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- payload
	}()

	select {
	case <-resultCh:
		t.Fatalf("read returned before data was written")
	case <-time.After(50 * time.Millisecond):
	}

	want := []byte("delayed message")
	if _, err := fs.Write(want); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("read error: %v", err)
	case payload := <-resultCh:
		if string(payload) != string(want) {
			t.Fatalf("expected %q, got %q", want, payload)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for read result")
	}
}
