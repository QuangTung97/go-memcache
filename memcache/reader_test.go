package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResponseReader_Empty(t *testing.T) {
	r := newResponseReader(10)
	size, ok := r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)
}

func TestResponseReader_Normal(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("abcd\r\nxxx\r\n"))

	size, ok := r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len("abcd\r\n"), size)

	data := make([]byte, size)
	r.readData(data)
	assert.Equal(t, []byte("abcd\r\n"), data)

	size, ok = r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len("xxx\r\n"), size)

	data = make([]byte, size)
	r.readData(data)
	assert.Equal(t, []byte("xxx\r\n"), data)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)
}

func TestResponseReader_Missing_LF(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("abcd\r"))

	size, ok := r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)

	r.recv([]byte("\n"))

	size, ok = r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len("abcd\r\n"), size)

	data := make([]byte, size)
	r.readData(data)
	assert.Equal(t, []byte("abcd\r\n"), data)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)
}

func TestResponseReader_Wrap_Around(t *testing.T) {
	r := newResponseReader(3)

	r.recv([]byte("abcd\r\n"))

	size, ok := r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len("abcd\r\n"), size)

	data := make([]byte, size)
	r.readData(data)
	assert.Equal(t, []byte("abcd\r\n"), data)

	r.recv([]byte("xxx\r\n"))
	size, ok = r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len("xxx\r\n"), size)

	data = make([]byte, size)
	r.readData(data)
	assert.Equal(t, []byte("xxx\r\n"), data)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)
}

func TestResponseReader_With_Value_Return(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA 13\r\nAA\r\n"))

	size, ok := r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)

	r.recv([]byte("123456789\r\nVA"))

	expected := "VA 13\r\nAA\r\n123456789\r\n"

	size, ok = r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len(expected), size)

	data := make([]byte, len(expected))
	r.readData(data)
	assert.Equal(t, []byte(expected), data)

	size, ok = r.getNext()
	assert.Equal(t, false, ok)

	// SECOND CYCLE

	r.recv([]byte("   4  \r\nabcd\r\n"))

	expected = "VA   4  \r\nabcd\r\n"

	size, ok = r.getNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, len(expected), size)

	data = make([]byte, len(expected))
	r.readData(data)
	assert.Equal(t, []byte(expected), data)
}

func TestResponseReader_With_VA_Error(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA xby\r\n"))

	size, ok := r.getNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, 0, size)
	assert.Equal(t, ErrBrokenPipe{reason: "not a number after VA"}, r.hasError())
}
