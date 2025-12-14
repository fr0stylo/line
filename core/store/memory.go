package store

import (
	"io"
	"sync"
)

type Memory struct {
	mux  *sync.Mutex
	cond *sync.Cond

	queue  [][]byte
	closed bool
}

func (m *Memory) Write(p []byte) (n int, err error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.closed {
		return 0, io.ErrClosedPipe
	}

	copied := append([]byte(nil), p...)
	m.queue = append(m.queue, copied)

	m.cond.Signal()

	return len(p), nil
}

func (m *Memory) Close() error {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.closed = true
	m.cond.Broadcast()
	return nil
}

func (m *Memory) MarshalJSON() ([]byte, error) {
	return []byte("0"), nil
}

func (m *Memory) UnmarshalJSON(bytes []byte) error {
	return nil
}

func (m *Memory) Read() (p []byte, err error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	for len(m.queue) == 0 && !m.closed {
		m.cond.Wait()
	}

	if len(m.queue) == 0 {
		return nil, io.EOF
	}
	blob := m.queue[0]
	m.queue = m.queue[1:]
	return blob, nil
}

func NewMemory() *Memory {
	mux := &sync.Mutex{}
	return &Memory{
		mux:    mux,
		cond:   sync.NewCond(mux),
		queue:  [][]byte{},
		closed: false,
	}
}
