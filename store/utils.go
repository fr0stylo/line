package store

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path"
)

func storeMetadata(filePath string, data any) error {
	if f, err := os.CreateTemp(path.Dir(filePath), "meta_*.tmp"); err == nil {
		defer f.Close()           //nolint:errcheck // temp file cleanup
		defer os.Remove(f.Name()) //nolint:errcheck // remove temp file
		if err := json.NewEncoder(f).Encode(data); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}

		if err := os.Rename(f.Name(), filePath); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}

func write(w io.Writer, p []byte) (n int, err error) {
	size := len(p)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(size))

	w1, err := w.Write(buf[:])
	if err != nil {
		return w1, err
	}

	w2, err := w.Write(p)

	return w1 + w2, err
}

func read(r io.Reader) (p []byte, n int, err error) {
	var buf [8]byte
	offsetN, err := r.Read(buf[:])
	if err != nil {
		return nil, offsetN, err
	}

	length := binary.BigEndian.Uint64(buf[:])
	blob := make([]byte, length)
	blobN, err := r.Read(blob)
	if err != nil {
		return nil, blobN + offsetN, err
	}

	return blob, blobN + offsetN, nil
}
