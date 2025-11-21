package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
)

// SegmentStore implements Store by rolling fixed-size log segments on disk.
type SegmentStore struct {
	dir          string
	metadataPath string

	writeSegment uint64
	readSegment  uint64

	readOffset     uint64
	writeOffset    uint64
	volatileOffset uint64

	segmentSize uint64

	mux  *sync.Mutex
	cond *sync.Cond

	w  *os.File
	wb *bufio.Writer

	r *os.File
}

// UnmarshalJSON restores metadata from the persisted JSON snapshot.
func (s *SegmentStore) UnmarshalJSON(bytes []byte) error {
	var data map[string]uint64
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}

	s.readOffset = data["readOffset"]
	s.writeOffset = data["writeOffset"]
	s.writeSegment = data["writeSegment"]
	s.readSegment = data["readSegment"]

	return nil
}

// MarshalJSON encodes the offsets and current segment indexes.
func (s *SegmentStore) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]uint64{
		"readOffset":   s.readOffset,
		"writeOffset":  s.writeOffset,
		"writeSegment": s.writeSegment,
		"readSegment":  s.readSegment,
	})
}

func (s *SegmentStore) nextWritableSegment() error {
	s.writeSegment++
	s.writeOffset = 0

	if err := s.w.Close(); err != nil {
		return err
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/%08d.log", s.dir, s.writeSegment), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	s.w = f
	s.wb = bufio.NewWriter(f)

	return nil
}

func (s *SegmentStore) nextReadableSegment() error {
	if err := s.r.Close(); err != nil {
		return err
	}

	if err := os.Remove(fmt.Sprintf("%s/%08d.log", s.dir, s.readSegment)); err != nil {
		return err
	}

	s.readSegment++
	s.readOffset = 0

	f, err := os.OpenFile(fmt.Sprintf("%s/%08d.log", s.dir, s.writeSegment), os.O_RDONLY, 0o644)
	if err != nil {
		return err
	}

	s.r = f

	return nil
}

// Write appends the payload to the active segment, rolling files and storing
// metadata when needed.
func (s *SegmentStore) Write(p []byte) (n int, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.writeOffset >= s.segmentSize {
		if err := s.nextWritableSegment(); err != nil {
			return 0, err
		}
	}

	n, err = write(s.wb, p)
	if err != nil {
		return 0, err
	}

	s.writeOffset += uint64(n)
	if err := storeMetadata(s.metadataPath, s); err != nil {
		return n, err
	}

	if err := s.wb.Flush(); err != nil {
		return 0, err
	}

	s.cond.Signal()

	return n, nil
}

// Read blocks until data is available, advancing to the next segment and
// pruning fully-read logs as needed.
func (s *SegmentStore) Read() (p []byte, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.readOffset == s.writeOffset && s.readSegment == s.writeSegment {
		s.cond.Wait()
	}

	if s.readOffset >= s.segmentSize {
		if err := s.nextReadableSegment(); err != nil {
			return nil, err
		}
	}

	blob, n, err := read(s.r)
	if err != nil {
		return nil, err
	}

	s.readOffset += uint64(n)
	if err := storeMetadata(s.metadataPath, s); err != nil {
		return blob, err
	}

	return blob, nil
}

// Close persists metadata and closes open segment descriptors.
func (s *SegmentStore) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	err := storeMetadata(s.metadataPath, s)
	if err != nil {
		return err
	}

	if err := s.r.Close(); err != nil {
		return err
	}

	return s.w.Close()
}

// NewSegmentStore constructs a segment-backed store rooted at dir with the
// provided segment size.
func NewSegmentStore(dir string, segmentSize uint64) (*SegmentStore, error) {
	mux := &sync.Mutex{}
	cond := sync.NewCond(mux)

	s := &SegmentStore{
		dir:          dir,
		metadataPath: dir + "_meta.json",

		segmentSize: segmentSize,

		mux:  mux,
		cond: cond,
	}
	if err := os.MkdirAll(path.Dir(dir), 0o755); err != nil {
		return nil, err
	}

	if meta, err := os.Open(s.metadataPath); err == nil {
		defer meta.Close() //nolint:errcheck // metadata file cleanup
		if err = json.NewDecoder(meta).Decode(s); err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/%08d.log", dir, s.writeSegment), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(int64(s.writeOffset), io.SeekStart); err != nil {
		return nil, err
	}

	s.w = f
	s.wb = bufio.NewWriter(f)

	r, err := os.OpenFile(fmt.Sprintf("%s/%08d.log", dir, s.readSegment), os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	if _, err := r.Seek(int64(s.readOffset), io.SeekStart); err != nil {
		return nil, err
	}

	s.r = r

	return s, nil
}
