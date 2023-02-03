package stats

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"strings"
	"testing"
)

type clientTest struct {
	client *Client
	nc     *connTest
}

type connTest struct {
	net.Conn
	reader io.Reader

	writeBytes []byte
	writeErr   error
}

func (c *connTest) Read(b []byte) (n int, err error) {
	return c.reader.Read(b)
}

func (c *connTest) Write(b []byte) (n int, err error) {
	c.writeBytes = append(c.writeBytes, b...)
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(b), nil
}

func newClientTest(t *testing.T, reader io.Reader) *clientTest {
	nc := &connTest{
		reader: reader,
	}
	c := &clientTest{
		nc: nc,
	}

	globalNetDial = func(network, address string) (net.Conn, error) {
		return nc, nil
	}
	defer func() { globalNetDial = net.Dial }()

	client, err := New("localhost:11211")
	assert.Equal(t, nil, err)

	c.client = client
	return c
}

func buildResponse(lines ...string) string {
	var buf strings.Builder
	for _, line := range lines {
		_, _ = buf.WriteString(line)
		_, _ = buf.WriteString("\r\n")
	}
	_, _ = buf.WriteString("END\r\n")
	return buf.String()
}

func TestStatsClient(t *testing.T) {
	t.Run("general", func(t *testing.T) {
		input := buildResponse(
			"STAT pid 12",
			"STAT uptime 444",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		general, err := c.client.GetGeneralStats()
		assert.Equal(t, nil, err)
		assert.Equal(t, GeneralStats{
			PID:    12,
			Uptime: 444,
		}, general)

		assert.Equal(t, []byte("stats\r\n"), c.nc.writeBytes)
	})

	t.Run("general-write-error", func(t *testing.T) {
		input := buildResponse(
			"STAT pid",
			"STAT uptime 444",
		)

		c := newClientTest(t, bytes.NewBuffer([]byte(input)))
		c.nc.writeErr = errors.New("some error")

		general, err := c.client.GetGeneralStats()
		assert.Equal(t, c.nc.writeErr, err)
		assert.Equal(t, GeneralStats{}, general)
	})

	t.Run("general-with-error", func(t *testing.T) {
		input := buildResponse(
			"STAT pid",
			"STAT uptime 444",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		general, err := c.client.GetGeneralStats()
		assert.Equal(t, NewError("missing stat fields"), err)
		assert.Equal(t, GeneralStats{}, general)
		assert.Equal(t, "stats: missing stat fields", err.Error())

		assert.Equal(t, []byte("stats\r\n"), c.nc.writeBytes)
	})

	t.Run("general-pid-not-number", func(t *testing.T) {
		input := buildResponse(
			"STAT pid aa",
			"STAT uptime 444",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		general, err := c.client.GetGeneralStats()
		assert.Error(t, err)
		assert.Equal(t, GeneralStats{}, general)
	})

	t.Run("general-uptime-not-number", func(t *testing.T) {
		input := buildResponse(
			"STAT pid 18",
			"STAT uptime cc",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		_, err := c.client.GetGeneralStats()
		assert.Error(t, err)
	})

	t.Run("slabs", func(t *testing.T) {
		input := buildResponse(
			"STAT 6:chunk_size 304",
			"STAT 6:chunks_per_page 3449",
			"STAT 6:total_pages 928",
			"STAT 6:total_chunks 3200672",
			"STAT 6:used_chunks 3200000",
			"STAT 6:free_chunks 672",
			"STAT 6:free_chunks_end 21",
			"STAT 6:get_hits 100000",
			"STAT 6:cmd_set 3200000",
			"STAT 6:delete_hits 22",
			"STAT 6:incr_hits 23",
			"STAT 6:decr_hits 24",
			"STAT 6:cas_hits 25",
			"STAT 6:cas_badval 26",
			"STAT 6:touch_hits 27",

			"STAT active_slabs 7",
			"STAT total_malloced 973078528",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		slabs, err := c.client.GetSlabsStats()
		assert.Equal(t, nil, err)
		assert.Equal(t, SlabsStats{
			ActiveSlabs:   7,
			TotalMalloced: 973078528,
			SlabIDs:       []uint32{6},
			Slabs: map[uint32]SingleSlabStats{
				6: {
					ChunkSize:     304,
					ChunksPerPage: 3449,
					TotalPages:    928,
					TotalChunks:   3200672,
				},
			},
		}, slabs)

		assert.Equal(t, []byte("stats slabs\r\n"), c.nc.writeBytes)
	})
}
