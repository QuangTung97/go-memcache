package memcache

import "fmt"

// ErrBrokenPipe ...
type ErrBrokenPipe struct {
	reason string
}

var _ error = ErrBrokenPipe{}

func (e ErrBrokenPipe) Error() string {
	return fmt.Sprintf("broken pipe: %s", e.reason)
}
