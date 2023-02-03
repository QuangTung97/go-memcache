package stats

import (
	"bufio"
	"github.com/QuangTung97/go-memcache/memcache"
	"io"
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

// New ...
func New(addr string) (*Client, error) {
	nc, err := netDialNewConn(addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		nc:     nc,
		parser: newStatsParser(nc.reader),
	}, nil
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
}

// SlabsStats ...
type SlabsStats struct {
	ActiveSlabs   uint32
	TotalMalloced uint64

	SlabIDs []uint32
	Slabs   map[uint32]SingleSlabStats
}

//revive:disable-next-line:cognitive-complexity
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
	closer io.Closer
	reader io.ReadCloser
}

func netDialNewConn(addr string) (netConn, error) {
	nc, err := globalNetDial("tcp", addr)
	if err != nil {
		return netConn{}, err
	}

	writer := bufio.NewWriterSize(nc, 4*1024)
	return netConn{
		reader: nc,
		writer: writer,
		closer: nc,
	}, nil
}

// Close ...
func (c *Client) Close() error {
	return c.nc.closer.Close()
}
