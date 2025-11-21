package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentStoreWriteReadRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "segments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create segment dir: %v", err)
	}
	ss, err := NewSegmentStore(dir, 1<<20)
	if err != nil {
		t.Fatalf("create segment store: %v", err)
	}
	t.Cleanup(func() {
		if err := ss.Close(); err != nil {
			t.Fatalf("close segment store: %v", err)
		}
	})

	payloads := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
	}

	for _, payload := range payloads {
		if _, err := ss.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}

	for i, want := range payloads {
		got, err := ss.Read()
		if err != nil {
			t.Fatalf("read payload %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("expected %q, got %q", want, got)
		}
	}
}

func TestSegmentStoreRolloverRemovesOldSegments(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "segments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create segment dir: %v", err)
	}
	const segmentSize = 32
	ss, err := NewSegmentStore(dir, segmentSize)
	if err != nil {
		t.Fatalf("create segment store: %v", err)
	}
	t.Cleanup(func() {
		if err := ss.Close(); err != nil {
			t.Fatalf("close segment store: %v", err)
		}
	})

	payload := bytes.Repeat([]byte("x"), 16) // entry size is 24 bytes including prefix
	for i := 0; i < 3; i++ {
		if _, err := ss.Write(payload); err != nil {
			t.Fatalf("write payload %d: %v", i, err)
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "00000001.log")); err != nil {
		t.Fatalf("expected rollover segment: %v", err)
	}

	for i := 0; i < 2; i++ {
		msg, err := ss.Read()
		if err != nil {
			t.Fatalf("read payload %d: %v", i, err)
		}
		if !bytes.Equal(msg, payload) {
			t.Fatalf("unexpected payload %q", msg)
		}
	}

	final, err := ss.Read()
	if err != nil {
		t.Fatalf("read final payload: %v", err)
	}
	if !bytes.Equal(final, payload) {
		t.Fatalf("unexpected final payload: %q", final)
	}

	if _, err := os.Stat(filepath.Join(dir, "00000000.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected first segment removed, err=%v", err)
	}
}

func TestSegmentStoreMetadataPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "segments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create segment dir: %v", err)
	}
	ss, err := NewSegmentStore(dir, 1<<10)
	if err != nil {
		t.Fatalf("create segment store: %v", err)
	}

	if _, err := ss.Write([]byte("first")); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if _, err := ss.Write([]byte("second")); err != nil {
		t.Fatalf("write second: %v", err)
	}

	read, err := ss.Read()
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if string(read) != "first" {
		t.Fatalf("expected first, got %q", read)
	}
	if err := ss.Close(); err != nil {
		t.Fatalf("close segment store: %v", err)
	}

	reopened, err := NewSegmentStore(dir, 1<<10)
	if err != nil {
		t.Fatalf("reopen segment store: %v", err)
	}
	defer reopened.Close() //nolint:errcheck // best effort cleanup

	next, err := reopened.Read()
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if string(next) != "second" {
		t.Fatalf("expected second, got %q", next)
	}
}
