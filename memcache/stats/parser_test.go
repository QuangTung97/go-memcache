package stats

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func newStatsParserTest(input string) *statsParser {
	scanner := bufio.NewScanner(bytes.NewBuffer([]byte(input)))
	return newStatsParser(scanner)
}

func TestStatsParser(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		input := "STAT pid 1\r\nSTAT uptime 14\r\nEND\r\n"
		p := newStatsParserTest(input)

		assert.Equal(t, true, p.next())
		assert.Equal(t, nil, p.getError())
		assert.Equal(t, statItem{
			key:   "pid",
			value: "1",
		}, p.getItem())

		assert.Equal(t, true, p.next())
		assert.Equal(t, nil, p.getError())
		assert.Equal(t, statItem{
			key:   "uptime",
			value: "14",
		}, p.getItem())

		assert.Equal(t, false, p.next())
		assert.Equal(t, nil, p.getError())
		assert.Equal(t, statItem{}, p.getItem())
	})

	t.Run("error-not-start-with-stats", func(t *testing.T) {
		input := "ERR pid 1\r\nSTAT uptime 14\r\nEND\r\n"
		p := newStatsParserTest(input)

		assert.Equal(t, false, p.next())
		assert.Equal(t, NewError("line not begin with STAT"), p.getError())
		assert.Equal(t, statItem{}, p.getItem())
	})

	t.Run("error-stats-missing-fields", func(t *testing.T) {
		input := "STAT pid\r\nSTAT uptime 14\r\nEND\r\n"
		p := newStatsParserTest(input)

		assert.Equal(t, false, p.next())
		assert.Equal(t, NewError("missing stat fields"), p.getError())
		assert.Equal(t, statItem{}, p.getItem())
	})

	t.Run("error-when-empty-line", func(t *testing.T) {
		input := "\r\n"
		p := newStatsParserTest(input)

		assert.Equal(t, false, p.next())
		assert.Equal(t, NewError("empty line"), p.getError())
		assert.Equal(t, statItem{}, p.getItem())
	})

	t.Run("error-EOF", func(t *testing.T) {
		input := ""
		p := newStatsParserTest(input)

		assert.Equal(t, false, p.next())
		assert.Equal(t, io.EOF, p.getError())
		assert.Equal(t, statItem{}, p.getItem())
	})
}

type testReader struct {
	readFunc func(p []byte) (n int, err error)
}

func (r *testReader) Read(p []byte) (n int, err error) {
	return r.readFunc(p)
}

func TestStatsParser_IO_Error(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		readErr := errors.New("read error")

		scanner := bufio.NewScanner(&testReader{
			readFunc: func(p []byte) (n int, err error) {
				return 0, readErr
			},
		})
		p := newStatsParser(scanner)

		assert.Equal(t, false, p.next())
		assert.Equal(t, readErr, p.getError())
		assert.Equal(t, statItem{}, p.getItem())
	})
}
