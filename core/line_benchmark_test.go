package core_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/fr0stylo/line/contracts/gen/envelope"
	line "github.com/fr0stylo/line/core"
	"github.com/fr0stylo/line/core/store"
)

func BenchmarkLinePushPopThroughput(b *testing.B) {
	payloadSizes := []int{128, 512, 2048, 8192}
	batchSizes := []int{1, 8, 64}

	for _, payloadSize := range payloadSizes {
		for _, batchSize := range batchSizes {
			ps, bs := payloadSize, batchSize
			name := fmt.Sprintf("%dB_batch%d", ps, bs)
			b.Run(name, func(b *testing.B) {
				benchmarkPushPop(b, ps, bs)
			})
		}
	}
}

func benchmarkPushPop(b *testing.B, payloadSize, batchSize int) {
	dir := filepath.Join(b.TempDir(), "segments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("failed to create segment directory: %v", err)
	}

	const segmentSize = 8 << 32 // 8 MiB segments keep rollover noise low
	s, err := store.NewSegmentStore(dir, segmentSize)
	if err != nil {
		b.Fatalf("failed to create segment store: %v", err)
	}

	q, err := line.NewLine(s)
	if err != nil {
		_ = s.Close()
		b.Fatalf("failed to create line queue: %v", err)
	}
	b.Cleanup(func() {
		if err := q.Close(); err != nil {
			b.Fatalf("failed to close line: %v", err)
		}
	})

	payload := bytes.Repeat([]byte("x"), payloadSize)
	totalBytes := payloadSize * batchSize
	b.SetBytes(int64(totalBytes))

	startCh := make(chan struct{})
	popErr := make(chan error, 1)
	go func() {
		<-startCh
		for i := 0; i < b.N*batchSize; i++ {
			if _, err := q.Pop(); err != nil {
				popErr <- fmt.Errorf("pop failed: %w", err)
				return
			}
		}
		popErr <- nil
	}()

	b.ResetTimer()
	close(startCh)

	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			if err := q.Push(&envelope.Envelope{Payload: payload}); err != nil {
				b.Fatalf("push failed: %v", err)
			}
		}
	}

	if err := <-popErr; err != nil {
		b.Fatal(err)
	}

	b.StopTimer()
	totalMsgs := b.N * batchSize
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(totalMsgs)/elapsed.Seconds(), "msgs/s")
	}
}
