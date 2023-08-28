package stats

import (
	"bufio"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

// Client is a client for statistics information & support dump all keys
// checkout https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// for more information
type Client struct {
	nc      netconn.NetConn
	scanner *bufio.Scanner
	parser  *statsParser
}

type dialConfig struct {
	errorLogger func(err error)

	connOptions []netconn.Option
}

// Option ...
type Option func(conf *dialConfig)

// WithErrorLogger ...
func WithErrorLogger(logger func(err error)) Option {
	return func(conf *dialConfig) {
		conf.errorLogger = logger
	}
}

// WithNetConnOptions ...
func WithNetConnOptions(options ...netconn.Option) Option {
	return func(conf *dialConfig) {
		conf.connOptions = options
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

	nc, err := netconn.DialNewConn(addr, conf.connOptions...)
	if err != nil {
		conf.errorLogger(err)
		nc = netconn.ErrorNetConn(err)
	}

	scanner := bufio.NewScanner(nc.Reader)

	return &Client{
		nc:      nc,
		scanner: scanner,
		parser:  newStatsParser(scanner),
	}
}

// GeneralStats ...
type GeneralStats struct {
	PID    uint64
	Uptime uint64
}

func (c *Client) writeCommand(cmd string) error {
	_, err := c.nc.Writer.Write([]byte(cmd))
	if err != nil {
		return err
	}

	return c.nc.Writer.Flush()
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

// SlabItemsStats ...
type SlabItemsStats struct {
	SlabIDs []uint32
	Slabs   map[uint32]SingleSlabItemStats
}

// SingleSlabItemStats ...
type SingleSlabItemStats struct {
	Number     uint64
	NumberHot  uint64
	NumberWarm uint64
	NumberCold uint64
	AgeHot     uint64
	AgeWarm    uint64
	Age        uint64

	MemRequested uint64

	Evicted        uint64
	EvictedNonZero uint64
	EvictedTime    uint64
	OutOfMemory    uint64
	TailRepairs    uint64
	Reclaimed      uint64

	ExpiredUnfetched uint64
	EvictedUnfetched uint64
	EvictedActive    uint64

	CrawlerReclaimed    uint64
	CrawlerItemsChecked uint64
	LRUTailRefLocked    uint64

	MovesToCold    uint64
	MovesToWarm    uint64
	MovesWithinLRU uint64

	DirectReclaims uint64
	HitsToHot      uint64
	HitsToWarm     uint64
	HitsToCold     uint64
	HitsToTemp     uint64
}

//revive:disable-next-line:cognitive-complexity,cyclomatic
func (s *SlabItemsStats) parseSlabItemStat(item statItem) error {
	fields := strings.Split(item.key, ":")
	if len(fields) < 3 {
		return NewError("missing slab item stat fields")
	}

	slabClassValue, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return err
	}
	slabClass := uint32(slabClassValue)

	setStat := func(fn func(s *SingleSlabItemStats)) {
		stats, existed := s.Slabs[slabClass]
		if !existed {
			s.SlabIDs = append(s.SlabIDs, slabClass)
		}
		fn(&stats)
		s.Slabs[slabClass] = stats
	}

	setNumber := func(fn func(s *SingleSlabItemStats, num uint64)) error {
		number, err := strconv.ParseUint(item.value, 10, 64)
		if err != nil {
			return err
		}
		setStat(func(s *SingleSlabItemStats) {
			fn(s, number)
		})
		return nil
	}

	switch fields[2] {
	case "number":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.Number = num })
	case "number_hot":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.NumberHot = num })
	case "number_warm":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.NumberWarm = num })
	case "number_cold":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.NumberCold = num })

	case "age_hot":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.AgeHot = num })
	case "age_warm":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.AgeWarm = num })
	case "age":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.Age = num })

	case "mem_requested":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.MemRequested = num })
	case "evicted":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.Evicted = num })
	case "evicted_nonzero":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.EvictedNonZero = num })
	case "evicted_time":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.EvictedTime = num })
	case "outofmemory":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.OutOfMemory = num })
	case "tailrepairs":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.TailRepairs = num })

	case "reclaimed":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.Reclaimed = num })
	case "expired_unfetched":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.ExpiredUnfetched = num })
	case "evicted_unfetched":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.EvictedUnfetched = num })
	case "evicted_active":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.EvictedActive = num })

	case "crawler_reclaimed":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.CrawlerReclaimed = num })
	case "crawler_items_checked":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.CrawlerItemsChecked = num })
	case "lrutail_reflocked":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.LRUTailRefLocked = num })

	case "moves_to_cold":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.MovesToCold = num })
	case "moves_to_warm":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.MovesToWarm = num })
	case "moves_within_lru":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.MovesWithinLRU = num })

	case "direct_reclaims":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.DirectReclaims = num })
	case "hits_to_hot":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.HitsToHot = num })
	case "hits_to_warm":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.HitsToWarm = num })
	case "hits_to_cold":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.HitsToCold = num })
	case "hits_to_temp":
		return setNumber(func(s *SingleSlabItemStats, num uint64) { s.HitsToTemp = num })

	default:
		return nil
	}
}

// GetSlabItemsStats ...
func (c *Client) GetSlabItemsStats() (SlabItemsStats, error) {
	if err := c.writeCommand("stats items\r\n"); err != nil {
		return SlabItemsStats{}, err
	}

	result := SlabItemsStats{
		Slabs: map[uint32]SingleSlabItemStats{},
	}

	for c.parser.next() {
		item := c.parser.getItem()
		err := result.parseSlabItemStat(item)
		if err != nil {
			return SlabItemsStats{}, err
		}
	}

	return result, c.parser.getError()
}

// MetaDumpKey ...
type MetaDumpKey struct {
	Key   string
	Exp   int64
	LA    int64
	CAS   uint64
	Fetch bool
	Class uint32
	Size  uint32
}

//revive:disable-next-line:cyclomatic,cognitive-complexity
func parseMetaDumpKey(line string) (MetaDumpKey, error) {
	kvList := strings.Split(line, " ")

	result := MetaDumpKey{}

	for _, kv := range kvList {
		keyValue := strings.Split(kv, "=")
		if len(keyValue) < 2 {
			return MetaDumpKey{}, NewError("missing key value")
		}

		value := keyValue[1]

		switch keyValue[0] {
		case "key":
			key, err := url.PathUnescape(value)
			if err != nil {
				return MetaDumpKey{}, err
			}
			result.Key = key

		case "exp":
			exp, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return MetaDumpKey{}, err
			}
			result.Exp = exp

		case "la":
			la, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return MetaDumpKey{}, err
			}
			result.LA = la

		case "cas":
			cas, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return MetaDumpKey{}, err
			}
			result.CAS = cas

		case "fetch":
			fetch := false
			if value == "yes" {
				fetch = true
			}
			result.Fetch = fetch

		case "cls":
			sizeClass, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return MetaDumpKey{}, err
			}
			result.Class = uint32(sizeClass)

		case "size":
			size, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return MetaDumpKey{}, err
			}
			result.Size = uint32(size)
		}
	}
	return result, nil
}

// MetaDumpAll ...
func (c *Client) MetaDumpAll(scanFunc func(key MetaDumpKey)) error {
	if err := c.writeCommand("lru_crawler metadump all\r\n"); err != nil {
		return err
	}

	for c.scanner.Scan() {
		line := c.scanner.Text()
		if line == "END" {
			return nil
		}

		key, err := parseMetaDumpKey(line)
		if err != nil {
			return err
		}
		scanFunc(key)
	}
	return c.scanner.Err()
}

type statItem struct {
	key   string
	value string
}

// Close ...
func (c *Client) Close() error {
	return c.nc.Closer.Close()
}
