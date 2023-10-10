package memcache

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreeCommandRequestData(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		cmd := newCommand()

		b := &requestBinaryEntry{
			data: []byte("data 02"),
		}
		a := &requestBinaryEntry{
			next: b,
			data: []byte("data 01"),
		}
		cmd.requestBinaries = a

		freeCommandRequestData(cmd)

		assert.Nil(t, cmd.requestData)
		assert.Nil(t, cmd.requestBinaries)
	})

	t.Run("check pool", func(t *testing.T) {
		cmd := newCommand()
		data := getByteSlice(4)
		copy(data, "ABCD")

		a := &requestBinaryEntry{
			data: data,
		}
		cmd.requestBinaries = a

		freeCommandRequestData(cmd)

		x := getByteSlice(4)
		x = x[:4]
		fmt.Println("NEW:", string(x))
	})
}

func TestWriteToWriter(t *testing.T) {
	t.Run("empty binary", func(t *testing.T) {
		cmd := newCommand()
		cmd.requestData = []byte("ms key01 8\r\n")

		var buf bytes.Buffer

		err := cmd.writeToWriter(&buf)
		assert.Equal(t, nil, err)

		assert.Equal(t, "ms key01 8\r\n", buf.String())
	})

	t.Run("single binary at the end", func(t *testing.T) {
		cmd := newCommand()
		cmd.requestData = []byte("ms key01 8\r\n")
		cmd.requestBinaries = &requestBinaryEntry{
			offset: len("ms key01 8\r\n"),
			data:   []byte("data01\r\n"),
		}

		var buf bytes.Buffer

		err := cmd.writeToWriter(&buf)
		assert.Equal(t, nil, err)

		assert.Equal(t, "ms key01 8\r\ndata01\r\n", buf.String())
	})

	t.Run("multiple binaries", func(t *testing.T) {
		cmd := newCommand()
		cmd.requestData = []byte("ms key01 8\r\nms key02 7 T20\r\n")

		b := &requestBinaryEntry{
			offset: len(cmd.requestData),
			data:   []byte("data-value02\r\n"),
		}
		a := &requestBinaryEntry{
			next:   b,
			offset: len("ms key01 8\r\n"),
			data:   []byte("data01\r\n"),
		}
		cmd.requestBinaries = a

		var buf bytes.Buffer

		err := cmd.writeToWriter(&buf)
		assert.Equal(t, nil, err)

		assert.Equal(t, "ms key01 8\r\ndata01\r\nms key02 7 T20\r\ndata-value02\r\n", buf.String())
	})

	t.Run("multiple binaries, last no binary", func(t *testing.T) {
		cmd := newCommand()
		cmd.requestData = []byte("ms key01 8\r\nms key02 7 T20\r\nmg key03 v\r\n")

		b := &requestBinaryEntry{
			offset: len("ms key01 8\r\nms key02 7 T20\r\n"),
			data:   []byte("data-value02\r\n"),
		}
		a := &requestBinaryEntry{
			next:   b,
			offset: len("ms key01 8\r\n"),
			data:   []byte("data01\r\n"),
		}
		cmd.requestBinaries = a

		var buf bytes.Buffer

		err := cmd.writeToWriter(&buf)
		assert.Equal(t, nil, err)

		assert.Equal(t,
			"ms key01 8\r\ndata01\r\n"+
				"ms key02 7 T20\r\ndata-value02\r\n"+
				"mg key03 v\r\n",
			buf.String())
	})
}
