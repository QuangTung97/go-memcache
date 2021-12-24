package memcache

import (
	"errors"
	"fmt"
)

// ErrBrokenPipe ...
type ErrBrokenPipe struct {
	reason string
}

var _ error = ErrBrokenPipe{}

// ErrConnClosed ...
var ErrConnClosed = errors.New("memcache: connection closed")

func (e ErrBrokenPipe) Error() string {
	return fmt.Sprintf("broken pipe: %s", e.reason)
}
