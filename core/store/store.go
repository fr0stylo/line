package store

import (
	"encoding/json"
	"io"
)

// Store defines the persistence contract queue implementations depend on.
type Store interface {
	io.Writer
	io.Closer
	json.Marshaler
	json.Unmarshaler
	Read() (p []byte, err error)
}
