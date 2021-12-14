package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResponseReader(t *testing.T) {
	r := newResponseReader()

	r.recv([]byte("abcd\r\nxxx\r\n"))

	assert.Equal(t, true, r.hasNext())

	size := len("abcd\r\n")
	assert.Equal(t, size, r.responseSize())

	data := make([]byte, size)
	r.readData(data)

	assert.Equal(t, []byte("abcd\r\n"), data)
}
