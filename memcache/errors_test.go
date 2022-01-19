package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestServerError(t *testing.T) {
	e := NewServerError("some error")
	assert.Equal(t, "server error: some error", e.Error())
}

func TestClientError(t *testing.T) {
	e := NewClientError("some error")
	assert.Equal(t, "client error: some error", e.Error())
}
