package stats

import (
	"bytes"
	"errors"
	"fmt"
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

func (c *connTest) Close() error {
	return nil
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

	client := New("localhost:11211")
	t.Cleanup(func() { _ = client.Close() })

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
					UsedChunks:    3200000,
				},
			},
		}, slabs)

		assert.Equal(t, []byte("stats slabs\r\n"), c.nc.writeBytes)
	})
}

func TestMemcache__Connect_Error(t *testing.T) {
	t.Run("without-logger", func(t *testing.T) {
		c := New("localhost:2288")
		defer func() { _ = c.Close() }()

		stats, err := c.GetSlabsStats()
		assert.Error(t, err)
		assert.Equal(t, SlabsStats{}, stats)
	})

	t.Run("with-logger", func(t *testing.T) {
		var logErr error
		c := New("localhost:2288", WithErrorLogger(func(err error) {
			logErr = err
		}))
		defer func() { _ = c.Close() }()

		assert.Error(t, logErr)

		stats, err := c.GetSlabsStats()
		assert.Equal(t, logErr, err)
		assert.Equal(t, SlabsStats{}, stats)
	})
}

func TestParseMetaDumpKey(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		key, err := parseMetaDumpKey(`key=KEY01 exp=-1 la=1675839370 cas=34234 fetch=yes cls=5 size=76`)
		assert.Equal(t, nil, err)
		assert.Equal(t, MetaDumpKey{
			Key:   "KEY01",
			Exp:   -1,
			LA:    1675839370,
			CAS:   34234,
			Fetch: true,
			Class: 5,
			Size:  76,
		}, key)
	})

	t.Run("invalid key value", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key`)
		assert.Equal(t, NewError("missing key value"), err)
	})

	t.Run("invalid exp", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key=KEY01 exp=a la=1675839370 cas=34234 fetch=yes cls=5 size=76`)
		assert.Error(t, err)
	})

	t.Run("invalid la", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key=KEY01 exp=-4 la=x cas=34234 fetch=yes cls=5 size=76`)
		assert.Error(t, err)
	})

	t.Run("invalid cas", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key=KEY01 exp=-4 la=8899 cas=u fetch=yes cls=5 size=76`)
		assert.Error(t, err)
	})

	t.Run("invalid cls", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key=KEY01 exp=-4 la=8899 cas=88 fetch=yes cls=x size=76`)
		assert.Error(t, err)
	})

	t.Run("invalid size", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key=KEY01 exp=-4 la=8899 cas=88 fetch=yes cls=7 size=u`)
		assert.Error(t, err)
	})
}

func TestStats_MetaDumpAll(t *testing.T) {
	c := New("localhost:11211")
	defer func() { _ = c.Close() }()

	err := c.MetaDumpAll(func(key MetaDumpKey) {
		fmt.Printf("%+v\n", key)
	})
	assert.Equal(t, nil, err)

	stats, err := c.GetSlabsStats()
	fmt.Println(stats, err)
}
