package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
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

const crcSize = 4

// Layout:
//  [4 bytes] blob length
//  [blob length bytes] blob
//  [4 bytes] CRC

func write(w io.Writer, p []byte) (n int, err error) {
	size := len(p)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(size+crcSize))

	w1, err := w.Write(buf[:])
	if err != nil {
		return w1, err
	}

	w2, err := w.Write(p)
	if err != nil {
		return w1 + w2, err
	}

	var crc [4]byte
	sum := crc32.Checksum(p, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(crc[:], sum)

	w3, err := w.Write(crc[:])
	if err != nil {
		return w1 + w2 + w3, err
	}

	return w1 + w2 + w3, err
}

var ErrorCRCCheckMismatch = errors.New("CRC check mismatch")

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

	crc := blob[len(blob)-4:]
	blob = blob[:len(blob)-4]

	if sum := crc32.Checksum(blob, crc32.MakeTable(crc32.Castagnoli)); sum != binary.BigEndian.Uint32(
		crc,
	) {
		return nil, blobN + offsetN, ErrorCRCCheckMismatch
	}

	return blob, blobN + offsetN, nil
}
