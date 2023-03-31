package memcache

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestResponseReader_Empty(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	ok := r.readNextData()
	assert.Equal(t, false, ok)
}

func TestResponseReader_Normal(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("abcd\r\nxxx\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("abcd\r\n"), cmd.responseData)

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("xxx\r\n"), cmd.responseData)

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	ok = r.readNextData()
	assert.Equal(t, false, ok)

	ok = r.readNextData()
	assert.Equal(t, false, ok)
}

func TestResponseReader_Missing_LF(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("abcd\r"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte("\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("abcd\r\n"), cmd.responseData)

	cmd = newCommand()
	r.setCurrentCommand(cmd)
	ok = r.readNextData()
	assert.Equal(t, false, ok)

	ok = r.readNextData()
	assert.Equal(t, false, ok)
}

func TestResponseReader_Wrap_Around(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("abcd\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("abcd\r\n"), cmd.responseData)

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("xxx\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("xxx\r\n"), cmd.responseData)

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	ok = r.readNextData()
	assert.Equal(t, false, ok)

	ok = r.readNextData()
	assert.Equal(t, false, ok)
}

func TestResponseReader_With_Value_Return(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 13\r\nAA\r\n"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte("123456789\r\nVA"))

	expected := "VA 13\r\n"

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte(expected), cmd.responseData)
	assert.Equal(t, [][]byte{
		[]byte("AA\r\n123456789"),
	}, cmd.responseBinaries)

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	ok = r.readNextData()
	assert.Equal(t, false, ok)

	// SECOND CYCLE

	r.recv([]byte("   4  \r\nabcd\r\n"))

	expected = "VA   4  \r\n"

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte(expected), cmd.responseData)
	assert.Equal(t, [][]byte{
		[]byte("abcd"),
	}, cmd.responseBinaries)
}

func TestResponseReader_With_VA_Error(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA xby\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, ErrBrokenPipe{reason: "not a number after VA"}, r.hasError())
}

func TestResponseReader_Reset_Simple(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 5\r\nXX"))

	r.reset()

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("EN\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("EN\r\n"), cmd.responseData)
}

func TestResponseReader_Reset_With_Wait_For_End(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 5\r\nXX"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, r.hasError())

	r.reset()

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("EN\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("EN\r\n"), cmd.responseData)
}

func TestResponseReader_Reset_With_Error(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA abcd\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, errNotNumberAfterVA, r.hasError())

	r.reset()

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("EN\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, r.hasError())
	assert.Equal(t, []byte("EN\r\n"), cmd.responseData)
}

func TestResponseReader_Client_Error(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("CLIENT_ERROR bad command line format\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("CLIENT_ERROR bad command line format\r\n"), cmd.responseData)

	cmd = newCommand()
	r.setCurrentCommand(cmd)

	ok = r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, []byte{}, cmd.responseData)
}

func TestResponseReader_No_A_After_V(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VB 12\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, errInvalidVA, r.hasError())
}

func TestResponseReader_No_LF_After_CR(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("HD W\r0"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, errNoLFAfterCR, r.hasError())
}

func TestResponseReader_No_LF_After_CR_For_VA(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 12\r0"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, errNoLFAfterCR, r.hasError())
}

func TestResponseReader_VA_With_Options(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 8 c123 X W\r"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte("\n12348888\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, r.hasError())
	assert.Equal(t, "VA 8 c123 X W\r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("12348888"),
	}, cmd.responseBinaries)
}

func TestResponseReader_Invalid_Reader_Usage(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("HD W\r\n"))

	assert.PanicsWithValue(t, "invalid reader usage", func() {
		r.recv([]byte("EN\r\n"))
	})
}

func TestResponseReader_Multi_Responses(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("HD W\r\nERROR some-error\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, "HD W\r\n", string(cmd.responseData))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, "HD W\r\nERROR some-error\r\n", string(cmd.responseData))
}

func TestResponseReader_State_Init__Content_Data_Disconnected(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("HD"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "HD", string(cmd.responseData))

	r.recv([]byte(" W c123 \r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, "HD W c123 \r\n", string(cmd.responseData))
}

func TestResponseReader_State_Find_First_Num__Content_Data_Disconnected(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA "))

	ok := r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA ", string(cmd.responseData))

	r.recv([]byte("3 \r\n"))

	ok = r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA 3 \r\n", string(cmd.responseData))

	r.recv([]byte("abc\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, "VA 3 \r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("abc"),
	}, cmd.responseBinaries)
}

func TestResponseReader_State_Get_Num__Content_Data_Disconnected(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 12"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA 12", string(cmd.responseData))

	r.recv([]byte("3 \r\nabc"))

	ok = r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA 123 \r\n", string(cmd.responseData))

	data := strings.Repeat("8", 120)

	r.recv([]byte(data + "\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)

	assert.Equal(t, "VA 123 \r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("abc" + data),
	}, cmd.responseBinaries)
}

func TestResponseReader_State_Find_First_CR_For_VA__Disconnected(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 3 "))

	ok := r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA 3 ", string(cmd.responseData))

	r.recv([]byte("\r\n"))

	ok = r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA 3 \r\n", string(cmd.responseData))

	r.recv([]byte("abc\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, "VA 3 \r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("abc"),
	}, cmd.responseBinaries)
}
