package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func (r *responseReader) readDataForTest() []byte {
	return r.readData(nil)
}

func TestResponseReader_Empty(t *testing.T) {
	r := newResponseReader(10)
	ok := r.hasNext()
	assert.Equal(t, false, ok)
}

func TestResponseReader_Normal(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("abcd\r\nxxx\r\n"))

	ok := r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte("abcd\r\n"), data)

	ok = r.hasNext()
	assert.Equal(t, true, ok)

	data = r.readDataForTest()
	assert.Equal(t, []byte("xxx\r\n"), data)

	ok = r.hasNext()
	assert.Equal(t, false, ok)

	ok = r.hasNext()
	assert.Equal(t, false, ok)
}

func TestResponseReader_Missing_LF(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("abcd\r"))

	ok := r.hasNext()
	assert.Equal(t, false, ok)

	r.recv([]byte("\n"))

	ok = r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte("abcd\r\n"), data)

	ok = r.hasNext()
	assert.Equal(t, false, ok)

	ok = r.hasNext()
	assert.Equal(t, false, ok)
}

func TestResponseReader_Wrap_Around(t *testing.T) {
	r := newResponseReader(3)

	r.recv([]byte("abcd\r\n"))

	ok := r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte("abcd\r\n"), data)

	r.recv([]byte("xxx\r\n"))

	ok = r.hasNext()
	assert.Equal(t, true, ok)

	data = r.readDataForTest()
	assert.Equal(t, []byte("xxx\r\n"), data)

	ok = r.hasNext()
	assert.Equal(t, false, ok)

	ok = r.hasNext()
	assert.Equal(t, false, ok)
}

func TestResponseReader_With_Value_Return(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA 13\r\nAA\r\n"))

	ok := r.hasNext()
	assert.Equal(t, false, ok)

	r.recv([]byte("123456789\r\nVA"))

	expected := "VA 13\r\nAA\r\n123456789\r\n"

	ok = r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte(expected), data)

	ok = r.hasNext()
	assert.Equal(t, false, ok)

	// SECOND CYCLE

	r.recv([]byte("   4  \r\nabcd\r\n"))

	expected = "VA   4  \r\nabcd\r\n"

	ok = r.hasNext()
	assert.Equal(t, true, ok)

	data = r.readDataForTest()
	assert.Equal(t, []byte(expected), data)
}

func TestResponseReader_With_VA_Error(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA xby\r\n"))

	ok := r.hasNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, ErrBrokenPipe{reason: "not a number after VA"}, r.hasError())
}

func TestResponseReader_Reset_Simple(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA 5\r\nXX"))

	r.reset()
	r.recv([]byte("EN\r\n"))

	ok := r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte("EN\r\n"), data)
}

func TestResponseReader_Reset_With_Wait_For_End(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA 5\r\nXX"))
	ok := r.hasNext()
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, r.hasError())

	r.reset()
	r.recv([]byte("EN\r\n"))

	ok = r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte("EN\r\n"), data)
}

func TestResponseReader_Reset_With_Error(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("VA abcd\r\n"))
	ok := r.hasNext()
	assert.Equal(t, false, ok)
	assert.Error(t, r.hasError())

	r.reset()
	r.recv([]byte("EN\r\n"))

	ok = r.hasNext()
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, r.hasError())

	data := r.readDataForTest()
	assert.Equal(t, []byte("EN\r\n"), data)
}

func TestResponseReader_Client_Error(t *testing.T) {
	r := newResponseReader(10)

	r.recv([]byte("CLIENT_ERROR bad command line format\r\n"))

	ok := r.hasNext()
	assert.Equal(t, true, ok)

	data := r.readDataForTest()
	assert.Equal(t, []byte("CLIENT_ERROR bad command line format\r\n"), data)

	ok = r.hasNext()
	assert.Equal(t, false, ok)
}
