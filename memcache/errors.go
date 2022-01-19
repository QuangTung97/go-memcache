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

// ErrServerError ...
type ErrServerError struct {
	Message string
}

func (e ErrServerError) Error() string {
	return fmt.Sprintf("server error: %s", e.Message)
}

// NewServerError ...
func NewServerError(msg string) error {
	return ErrServerError{Message: msg}
}

// ErrClientError ...
type ErrClientError struct {
	Message string
}

func (e ErrClientError) Error() string {
	return fmt.Sprintf("client error: %s", e.Message)
}

// NewClientError ...
func NewClientError(msg string) error {
	return ErrClientError{Message: msg}
}
