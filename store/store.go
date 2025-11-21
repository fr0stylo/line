package store

import (
	"encoding/json"
	"io"
)

const offsetSize uint64 = 8

type Store interface {
	io.Writer
	io.Closer
	json.Marshaler
	json.Unmarshaler
	Read() (p []byte, err error)
}
