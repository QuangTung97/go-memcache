package memcache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerError(t *testing.T) {
	e := NewServerError("some error")
	assert.Equal(t, "server error: some error", e.Error())
}

func TestClientError(t *testing.T) {
	e := NewClientError("some error")
	assert.Equal(t, "client error: some error", e.Error())
}

func TestBrokenPipeError(t *testing.T) {
	e := ErrBrokenPipe{reason: "some reason"}
	assert.Equal(t, "broken pipe: some reason", e.Error())
}

func TestIsErrorMessage(t *testing.T) {
	b := IsServerError(nil)
	assert.Equal(t, false, b)

	b = IsServerError(errors.New("new error"))
	assert.Equal(t, false, b)

	b = IsServerError(NewServerError("some error"))
	assert.Equal(t, true, b)
}
