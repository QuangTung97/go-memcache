package memcache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"testing"
	"time"
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

func TestResponseReader_With_Single_Digit(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 3\r\nAAC\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)

	expected := "VA 3\r\n"

	assert.Equal(t, []byte(expected), cmd.responseData)
	assert.Equal(t, [][]byte{
		[]byte("AAC"),
	}, cmd.responseBinaries)
}

func TestResponseReader_With_Single_Digit__Two_Responses(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 3\r\nAAC\r\nVA 4\r\nKKKK\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)

	expected := "VA 3\r\n"
	assert.Equal(t, []byte(expected), cmd.responseData)
	assert.Equal(t, [][]byte{
		[]byte("AAC"),
	}, cmd.responseBinaries)

	// read again
	ok = r.readNextData()
	assert.Equal(t, true, ok)

	expected = "VA 3\r\nVA 4\r\n"
	assert.Equal(t, expected, string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("AAC"),
		[]byte("KKKK"),
	}, cmd.responseBinaries)
}

func TestResponseReader_With_Data_CR_And_LF_Discontinued(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 3\r\nAAC\r"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte("\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)

	expected := "VA 3\r\n"

	assert.Equal(t, []byte(expected), cmd.responseData)
	assert.Equal(t, [][]byte{
		[]byte("AAC"),
	}, cmd.responseBinaries)
}

func TestResponseReader_With_Data_Zero(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("VA 0\r\n\r\n"))

	ok := r.readNextData()
	assert.Equal(t, true, ok)

	expected := "VA 0\r\n"

	assert.Equal(t, []byte(expected), cmd.responseData)
	assert.Equal(t, [][]byte{
		[]byte(""),
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
	assert.Equal(t, nil, r.hasError())
	assert.Equal(t, "VB 12\r\n", string(cmd.responseData))
	assert.Equal(t, 0, len(cmd.responseBinaries))
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

func TestResponseReader_With_Empty(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv(nil)
	ok := r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte("HD W\r\n"))
	ok = r.readNextData()
	assert.Equal(t, true, ok)

	assert.Equal(t, nil, r.hasError())
	assert.Equal(t, "HD W\r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte(nil), cmd.responseBinaries)
}

func TestResponseReader_With_VA_Inside_Simple_Response(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("HD "))
	ok := r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte("VA\r\n"))
	ok = r.readNextData()
	assert.Equal(t, true, ok)

	assert.Equal(t, nil, r.hasError())
	assert.Equal(t, "HD VA\r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte(nil), cmd.responseBinaries)
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

func TestResponseReader_State_Find_A_Content_Data_Disconnected(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	r.recv([]byte("V"))

	ok := r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "V", string(cmd.responseData))

	r.recv([]byte("A  3 \r\n"))

	ok = r.readNextData()
	assert.Equal(t, false, ok)
	assert.Equal(t, "VA  3 \r\n", string(cmd.responseData))

	r.recv([]byte("abc\r\n"))

	ok = r.readNextData()
	assert.Equal(t, true, ok)
	assert.Equal(t, "VA  3 \r\n", string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("abc"),
	}, cmd.responseBinaries)
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

//revive:disable-next-line:cognitive-complexity
func TestResponseReader_Random_Disconnected(t *testing.T) {
	seed := time.Now().UnixNano()
	fmt.Println("SEED:", seed)
	rand.Seed(seed)

	for i := 0; i < 10000; i++ {
		r := newResponseReader()

		cmd := newCommand()
		r.setCurrentCommand(cmd)

		resp := "VA 3\r\nABC\r\nSTORED\r\nVA 4 c123\r\n1234\r\nEN\r\n"

		splitIndex := rand.Intn(len(resp))
		first := resp[:splitIndex]
		second := resp[splitIndex:]

		recvTimes := 0
		cmdCount := 0

	Outer:
		for cmdCount < 4 {
			for {
				ok := r.readNextData()
				if ok {
					cmdCount++
					continue Outer
				}

				if recvTimes == 0 {
					r.recv([]byte(first))
				} else if recvTimes == 1 {
					r.recv([]byte(second))
				} else {
					panic("Invalid rec times")
				}
				recvTimes++
			}
		}

		expected := "VA 3\r\nSTORED\r\nVA 4 c123\r\nEN\r\n"
		assert.Equal(t, expected, string(cmd.responseData))

		if expected != string(cmd.responseData) {
			fmt.Println(splitIndex)
		}

		assert.Equal(t, [][]byte{
			[]byte("ABC"),
			[]byte("1234"),
		}, cmd.responseBinaries)
	}
}

func TestResponseReader_Fix_Bug_Loop_Infinite_On_ReadNextData(t *testing.T) {
	r := newResponseReader()

	cmd := newCommand()
	r.setCurrentCommand(cmd)

	first := "VA 3\r\nABC\r\nS"
	second := "TORED\r\nVA 4 c123\r\n1234\r\nEN\r\n"

	r.recv([]byte(first))

	ok := r.readNextData()
	assert.Equal(t, true, ok)

	ok = r.readNextData()
	assert.Equal(t, false, ok)

	r.recv([]byte(second))

	ok = r.readNextData()
	assert.Equal(t, true, ok)

	ok = r.readNextData()
	assert.Equal(t, true, ok)

	ok = r.readNextData()
	assert.Equal(t, true, ok)

	ok = r.readNextData()
	assert.Equal(t, false, ok)

	expected := "VA 3\r\nSTORED\r\nVA 4 c123\r\nEN\r\n"
	assert.Equal(t, expected, string(cmd.responseData))

	assert.Equal(t, [][]byte{
		[]byte("ABC"),
		[]byte("1234"),
	}, cmd.responseBinaries)
}

func readAllFromReader(r *responseReader, data [][]byte) {
	for {
		ok := r.readNextData()
		if ok {
			continue
		}

		if len(data) == 0 {
			return
		}
		r.recv(data[0])
		data = data[1:]
	}
}

func TestResponseReader_Split_At_Arbitrary_Point(t *testing.T) {
	fullCmd := "HD 123 VA\r\nVA 3 XWVA\r\nVA1\r\nEN XVA\r\nVA 4  \r\nabVA\r\n"

	for i := range fullCmd {
		r := newResponseReader()
		cmd := newCommand()
		r.setCurrentCommand(cmd)

		first := []byte(fullCmd[:i])
		second := []byte(fullCmd[i:])

		readAllFromReader(r, [][]byte{first, second})

		// Check error and response data
		assert.Equal(t, nil, r.hasError())
		assert.Equal(t, "HD 123 VA\r\nVA 3 XWVA\r\nEN XVA\r\nVA 4  \r\n", string(cmd.responseData))
		assert.Equal(t, [][]byte{
			[]byte("VA1"),
			[]byte("abVA"),
		}, cmd.responseBinaries)
	}
}
