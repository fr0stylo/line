package store

import (
	"encoding/json"
	"io"
	"os"
	"path"
	"sync"
)

type FileStore struct {
	path         string
	metadataPath string

	mux  *sync.Mutex
	cond *sync.Cond

	readOffset  uint64
	writeOffset uint64

	w *os.File
	r *os.File
}

func (f *FileStore) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"readOffset":  f.readOffset,
		"writeOffset": f.writeOffset,
	})
}

func (f *FileStore) UnmarshalJSON(bytes []byte) error {
	var data map[string]uint64
	if err := json.Unmarshal(bytes, &data); err != nil {
		return err
	}
	f.readOffset = data["readOffset"]
	f.writeOffset = data["writeOffset"]
	return nil
}

func (f *FileStore) Write(p []byte) (n int, err error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	n, err = write(f.w, p)
	if err != nil {
		return n, err
	}
	f.writeOffset += uint64(n)

	if err := storeMetadata(f.metadataPath, f); err != nil {
		return n, err
	}
	f.cond.Signal()

	return n, f.w.Sync()
}

func (f *FileStore) readSingle() ([]byte, error) {
	blob, n, err := read(f.r)
	if err != nil {
		return nil, err
	}

	f.readOffset += offsetSize + uint64(n)

	if err = storeMetadata(f.metadataPath, f); err != nil {
		return nil, err
	}
	return blob, nil
}

func (f *FileStore) Read() (p []byte, err error) {
	f.mux.Lock()
	defer f.mux.Unlock()
	for f.readOffset >= f.writeOffset {
		f.cond.Wait()
	}

	return f.readSingle()
}

func (f *FileStore) Close() error {
	if err := storeMetadata(f.metadataPath, f); err != nil {
		return err
	}
	if err := f.r.Close(); err != nil {
		return err
	}
	return f.w.Close()
}

func NewFile(filePath string) (*FileStore, error) {
	if err := os.MkdirAll(path.Dir(filePath), 0o755); err != nil {
		return nil, err
	}

	mux := &sync.Mutex{}
	f := &FileStore{
		path:         filePath,
		mux:          mux,
		cond:         sync.NewCond(mux),
		metadataPath: filePath + "_meta.json",
	}

	if meta, err := os.Open(f.metadataPath); err == nil {
		defer meta.Close() //nolint:errcheck // metadata file cleanup
		if err = json.NewDecoder(meta).Decode(f); err != nil {
			return nil, err
		}
	}

	w, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	f.w = w
	if _, err := f.w.Seek(int64(f.writeOffset), io.SeekStart); err != nil {
		return nil, err
	}

	r, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	f.r = r

	if _, err := f.r.Seek(int64(f.readOffset), io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}
