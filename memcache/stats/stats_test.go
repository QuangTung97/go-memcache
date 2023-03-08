package stats

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache/netconn"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"strings"
	"testing"
	"time"
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

	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *connTest) SetReadDeadline(t time.Time) error {
	c.readTimeout = t.Sub(time.Now())
	return nil
}

func (c *connTest) SetWriteDeadline(t time.Time) error {
	c.writeTimeout = t.Sub(time.Now())
	return nil
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

	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		return nc, nil
	}

	client := New("localhost:11211", WithNetConnOptions(netconn.WithDialFunc(dialFunc)))
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

		fmt.Println(c.nc.writeTimeout)
		assert.Greater(t, c.nc.writeTimeout, 900*time.Millisecond)

		fmt.Println(c.nc.readTimeout)
		assert.Greater(t, c.nc.readTimeout, 900*time.Millisecond)
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

	t.Run("items", func(t *testing.T) {
		input := buildResponse(
			"STAT items:6:number 3200000",
			"STAT items:6:number_hot 78",
			"STAT items:6:number_warm 800000",
			"STAT items:6:number_cold 2400000",
			"STAT items:6:age_hot 7",
			"STAT items:6:age_warm 18",
			"STAT items:6:age 42",
			"STAT items:6:mem_requested 886400000",
			"STAT items:6:evicted 11",
			"STAT items:6:evicted_nonzero 12",
			"STAT items:6:evicted_time 13",
			"STAT items:6:outofmemory 5",
			"STAT items:6:tailrepairs 15",
			"STAT items:6:reclaimed 3177700",
			"STAT items:6:expired_unfetched 2384166",
			"STAT items:6:evicted_unfetched 7798",
			"STAT items:6:evicted_active 47",
			"STAT items:6:crawler_reclaimed 31",
			"STAT items:6:crawler_items_checked 32",
			"STAT items:6:lrutail_reflocked 24092",
			"STAT items:6:moves_to_cold 6190520",
			"STAT items:6:moves_to_warm 1600000",
			"STAT items:6:moves_within_lru 12193254",
			"STAT items:6:direct_reclaims 71",
			"STAT items:6:hits_to_hot 815190",
			"STAT items:6:hits_to_warm 12402747",
			"STAT items:6:hits_to_cold 2782063",
			"STAT items:6:hits_to_temp 77",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		slabs, err := c.client.GetSlabItemsStats()
		assert.Equal(t, nil, err)
		assert.Equal(t, SlabItemsStats{
			SlabIDs: []uint32{6},
			Slabs: map[uint32]SingleSlabItemStats{
				6: {
					Number:     3200000,
					NumberHot:  78,
					NumberWarm: 800000,
					NumberCold: 2400000,

					AgeHot:  7,
					AgeWarm: 18,
					Age:     42,

					MemRequested: 886400000,

					Evicted:        11,
					EvictedNonZero: 12,
					EvictedTime:    13,

					OutOfMemory: 5,
					TailRepairs: 15,

					Reclaimed:        3177700,
					ExpiredUnfetched: 2384166,
					EvictedUnfetched: 7798,
					EvictedActive:    47,

					CrawlerReclaimed:    31,
					CrawlerItemsChecked: 32,
					LRUTailRefLocked:    24092,

					MovesToCold:    6190520,
					MovesToWarm:    1600000,
					MovesWithinLRU: 12193254,

					DirectReclaims: 71,
					HitsToHot:      815190,
					HitsToWarm:     12402747,
					HitsToCold:     2782063,
					HitsToTemp:     77,
				},
			},
		}, slabs)

		assert.Equal(t, []byte("stats items\r\n"), c.nc.writeBytes)
	})

	t.Run("items error", func(t *testing.T) {
		input := buildResponse(
			"STAT items:6 3200000",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		slabs, err := c.client.GetSlabItemsStats()
		assert.Equal(t, NewError("missing slab item stat fields"), err)
		assert.Equal(t, SlabItemsStats{}, slabs)

		assert.Equal(t, []byte("stats items\r\n"), c.nc.writeBytes)
	})

	t.Run("items slab class not number", func(t *testing.T) {
		input := buildResponse(
			"STAT items:a:number 3200000",
		)
		c := newClientTest(t, bytes.NewBuffer([]byte(input)))

		slabs, err := c.client.GetSlabItemsStats()
		assert.Error(t, err)
		assert.Equal(t, SlabItemsStats{}, slabs)

		assert.Equal(t, []byte("stats items\r\n"), c.nc.writeBytes)
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

	t.Run("key-with-escaped", func(t *testing.T) {
		key, err := parseMetaDumpKey(`key=KEY01%3A40 exp=-1 la=1675839370 cas=34234 fetch=yes cls=5 size=76`)
		assert.Equal(t, nil, err)
		assert.Equal(t, MetaDumpKey{
			Key:   "KEY01:40",
			Exp:   -1,
			LA:    1675839370,
			CAS:   34234,
			Fetch: true,
			Class: 5,
			Size:  76,
		}, key)
	})

	t.Run("key-with-plus", func(t *testing.T) {
		key, err := parseMetaDumpKey(`key=KEY01%2B40 exp=-1 la=1675839370 cas=34234 fetch=yes cls=5 size=76`)
		assert.Equal(t, nil, err)
		assert.Equal(t, MetaDumpKey{
			Key:   "KEY01+40",
			Exp:   -1,
			LA:    1675839370,
			CAS:   34234,
			Fetch: true,
			Class: 5,
			Size:  76,
		}, key)
	})

	t.Run("invalid key escaped", func(t *testing.T) {
		_, err := parseMetaDumpKey(`key=tung%mm exp=-1 la=1675839370 cas=34234 fetch=yes cls=5 size=76`)
		assert.Error(t, err)
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
