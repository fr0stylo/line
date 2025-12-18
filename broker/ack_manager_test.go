package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAckManager_Flow(t *testing.T) {
	am := NewAckManager(WithMaxRequeueLength(10))
	msgID := "test-msg-1"
	msgContent := "hello"

	// 1. Initiate
	am.Initiate(msgID, msgContent)

	// 2. Ack
	am.Ack(msgID)

	// Verify it's gone from internal map (via Nack attempt)
	am.Nack(msgID)
	assert.Nil(t, am.Pop(), "Should not be able to pop a message that was acked")
}

func TestAckManager_NackAndPop(t *testing.T) {
	am := NewAckManager(WithMaxRequeueLength(10))
	msgID := "test-msg-2"
	msgContent := "world"

	am.Initiate(msgID, msgContent)

	// Nack should move it to the requeue channel
	am.Nack(msgID)

	// Pop should retrieve it
	popped := am.Pop()
	assert.Equal(t, msgContent, popped)

	// Second pop should be nil
	assert.Nil(t, am.Pop())
}

func TestAckManager_Concurrency(t *testing.T) {
	// Test that Nack doesn't block the caller when space is available
	am := NewAckManager(WithMaxRequeueLength(1))
	am.Initiate("1", "msg1")

	done := make(chan bool)
	go func() {
		am.Nack("1")
		done <- true
	}()

	select {
	case <-done:
		// Success: Nack didn't block indefinitely
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Nack blocked unexpectedly")
	}

	assert.Equal(t, "msg1", am.Pop())
}

func TestAckManager_EmptyPop(t *testing.T) {
	am := NewAckManager()
	assert.Nil(t, am.Pop(), "Pop on empty manager should return nil")
}

func TestAckManager_Options(t *testing.T) {
	am := NewAckManager(WithMaxRequeueLength(5))
	// Internal check of capacity if we could, but we can verify behavior
	for i := 0; i < 5; i++ {
		am.Initiate(string(rune(i)), i)
		am.Nack(string(rune(i)))
	}

	// This 6th one would block if the channel was unbuffered or size 5 and we didn't pop.
	// Since we are testing if it blocks, we can use a goroutine.
	finished := make(chan bool, 1)
	go func() {
		am.Initiate("6", 6)
		am.Nack("6")
		finished <- true
	}()

	select {
	case <-finished:
		t.Fatal("Nack should have blocked because channel is full")
	case <-time.After(50 * time.Millisecond):
		// Expected: it's blocked
	}
}
