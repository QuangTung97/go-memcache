package stats

import (
	"bufio"
	"github.com/QuangTung97/go-memcache/memcache"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

// Client is a client for statistics information
// checkout https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// for more information
type Client struct {
	nc     netConn
	parser *statsParser
}

// for testing
var globalNetDial = net.Dial

type dialConfig struct {
	errorLogger func(err error)
}

// Option ...
type Option func(conf *dialConfig)

// WithErrorLogger ...
func WithErrorLogger(logger func(err error)) Option {
	return func(conf *dialConfig) {
		conf.errorLogger = logger
	}
}

// New ...
func New(addr string, options ...Option) *Client {
	conf := &dialConfig{
		errorLogger: func(err error) {
			log.Println("[ERROR] Dial memcache for stats with error:", err)
		},
	}

	for _, opt := range options {
		opt(conf)
	}

	nc := netDialNewConn(addr, conf)

	return &Client{
		nc:     nc,
		parser: newStatsParser(nc.reader),
	}
}

// GeneralStats ...
type GeneralStats struct {
	PID    uint64
	Uptime uint64
}

func (c *Client) writeCommand(cmd string) error {
	_, err := c.nc.writer.Write([]byte(cmd))
	if err != nil {
		return err
	}

	return c.nc.writer.Flush()
}

// GetGeneralStats ...
func (c *Client) GetGeneralStats() (GeneralStats, error) {
	if err := c.writeCommand("stats\r\n"); err != nil {
		return GeneralStats{}, err
	}

	result := GeneralStats{}

	for c.parser.next() {
		item := c.parser.getItem()
		switch item.key {
		case "pid":
			pid, err := strconv.ParseUint(item.value, 10, 64)
			if err != nil {
				return GeneralStats{}, err
			}
			result.PID = pid

		case "uptime":
			uptime, err := strconv.ParseUint(item.value, 10, 64)
			if err != nil {
				return GeneralStats{}, err
			}
			result.Uptime = uptime

		default:
		}
	}

	return result, c.parser.getError()
}

// SingleSlabStats ...
type SingleSlabStats struct {
	ChunkSize     uint32
	ChunksPerPage uint32
	TotalPages    uint32
	TotalChunks   uint64
	UsedChunks    uint64
}

// SlabsStats ...
type SlabsStats struct {
	ActiveSlabs   uint32
	TotalMalloced uint64

	SlabIDs []uint32
	Slabs   map[uint32]SingleSlabStats
}

//revive:disable-next-line:cognitive-complexity,cyclomatic
func (s *SlabsStats) parseSlabStat(item statItem) error {
	fields := strings.Split(item.key, ":")
	if len(fields) < 2 {
		return NewError("missing stab stat fields")
	}

	slabClassValue, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return err
	}
	slabClass := uint32(slabClassValue)

	setStat := func(fn func(s *SingleSlabStats)) {
		stats, existed := s.Slabs[slabClass]
		if !existed {
			s.SlabIDs = append(s.SlabIDs, slabClass)
		}
		fn(&stats)
		s.Slabs[slabClass] = stats
	}

	switch fields[1] {
	case "chunk_size":
		chunkSize, err := strconv.ParseUint(item.value, 10, 32)
		if err != nil {
			return err
		}
		setStat(func(s *SingleSlabStats) { s.ChunkSize = uint32(chunkSize) })

	case "chunks_per_page":
		chunksPerPage, err := strconv.ParseUint(item.value, 10, 32)
		if err != nil {
			return err
		}
		setStat(func(s *SingleSlabStats) { s.ChunksPerPage = uint32(chunksPerPage) })

	case "total_pages":
		totalPages, err := strconv.ParseUint(item.value, 10, 32)
		if err != nil {
			return err
		}
		setStat(func(s *SingleSlabStats) { s.TotalPages = uint32(totalPages) })

	case "total_chunks":
		totalChunks, err := strconv.ParseUint(item.value, 10, 64)
		if err != nil {
			return err
		}
		setStat(func(s *SingleSlabStats) { s.TotalChunks = totalChunks })

	case "used_chunks":
		usedChunks, err := strconv.ParseUint(item.value, 10, 64)
		if err != nil {
			return err
		}
		setStat(func(s *SingleSlabStats) { s.UsedChunks = usedChunks })

	default:
	}

	return nil
}

// GetSlabsStats ...
func (c *Client) GetSlabsStats() (SlabsStats, error) {
	if err := c.writeCommand("stats slabs\r\n"); err != nil {
		return SlabsStats{}, err
	}

	result := SlabsStats{
		Slabs: map[uint32]SingleSlabStats{},
	}

	for c.parser.next() {
		item := c.parser.getItem()
		switch item.key {
		case "active_slabs":
			active, err := strconv.ParseUint(item.value, 10, 32)
			if err != nil {
				return SlabsStats{}, err
			}
			result.ActiveSlabs = uint32(active)

		case "total_malloced":
			totalMalloced, err := strconv.ParseUint(item.value, 10, 64)
			if err != nil {
				return SlabsStats{}, err
			}
			result.TotalMalloced = totalMalloced

		default:
			err := result.parseSlabStat(item)
			if err != nil {
				return SlabsStats{}, err
			}
		}
	}

	return result, c.parser.getError()
}

type statItem struct {
	key   string
	value string
}

type netConn struct {
	writer memcache.FlushWriter
	reader io.ReadCloser
}

type errorConn struct {
	err error
}

func (w *errorConn) Write([]byte) (n int, err error) {
	return 0, w.err
}

func (w *errorConn) Read([]byte) (n int, err error) {
	return 0, w.err
}

func errorNetConn(err error) netConn {
	conn := &errorConn{
		err: err,
	}
	return netConn{
		writer: memcache.NoopFlusher(conn),
		reader: io.NopCloser(conn),
	}
}

func netDialNewConn(addr string, conf *dialConfig) netConn {
	nc, err := globalNetDial("tcp", addr)
	if err != nil {
		conf.errorLogger(err)
		return errorNetConn(err)
	}

	writer := bufio.NewWriterSize(nc, 4*1024)
	return netConn{
		reader: nc,
		writer: writer,
	}
}

// Close ...
func (c *Client) Close() error {
	return c.nc.reader.Close()
}
