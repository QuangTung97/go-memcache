package memcache

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type kvStore struct {
	mut    sync.Mutex
	kv     map[string]uint64
	maxKey int
	client *Client
}

func keyFunc(i int) string {
	return fmt.Sprintf("%07d", i)
}

func valueFunc(key string, v uint64) []byte {
	return []byte(fmt.Sprintf("%s:%d", key, v))
}

func newKvStore(num int, client *Client) *kvStore {
	kv := map[string]uint64{}
	for i := 0; i < num; i++ {
		key := keyFunc(i)
		kv[key] = 1
	}
	return &kvStore{
		kv:     kv,
		client: client,
		maxKey: num,
	}
}

func (s *kvStore) randKey() string {
	return keyFunc(rand.Intn(s.maxKey))
}

func (s *kvStore) mgetFromCache() {
	p := s.client.Pipeline()
	defer p.Finish()

	key := s.randKey()

	resp, err := p.MGet(key, MGetOptions{
		N:   10,
		CAS: true,
	})()
	if err != nil {
		panic(err)
	}

	if resp.Type != MGetResponseTypeVA {
		panic("Invalid response type")
	}

	if resp.Flags&MGetFlagZ > 0 {
		return
	}

	if resp.Flags&MGetFlagW == 0 {
		return
	}

	s.mut.Lock()
	v := s.kv[key]
	s.mut.Unlock()

	time.Sleep(100 * time.Microsecond)

	value := valueFunc(key, v)

	_, err = p.MSet(key, value, MSetOptions{
		CAS: resp.CAS,
	})()
	if err != nil {
		panic(err)
	}
}

func (s *kvStore) updateStore() {
	key := s.randKey()

	s.mut.Lock()
	v := s.kv[key]
	v++
	s.kv[key] = v
	s.mut.Unlock()

	p := s.client.Pipeline()
	defer p.Finish()

	_, err := p.MDel(key, MDelOptions{})()
	if err != nil {
		panic(err)
	}
}

func (s *kvStore) flushAll() {
	p := s.client.Pipeline()
	defer p.Finish()

	for k := 0; k < s.maxKey; k++ {
		key := keyFunc(k)
		p.MDel(key, MDelOptions{})
	}
}

//revive:disable:cognitive-complexity
func consistentCacheSingleLoop(t *testing.T, client *Client) {
	const numKeys = 3
	const numReadThreads = 2
	const numWriteThreads = 2
	const numLoops = 10

	s := newKvStore(numKeys, client)
	s.flushAll()

	var wg sync.WaitGroup
	wg.Add(numReadThreads + numWriteThreads)
	for tid := 0; tid < numWriteThreads; tid++ {
		go func() {
			defer wg.Done()

			for n := 0; n < numLoops; n++ {
				s.updateStore()
			}
		}()
	}

	for tid := 0; tid < numReadThreads; tid++ {
		go func() {
			defer wg.Done()

			for n := 0; n < numLoops; n++ {
				s.mgetFromCache()
			}
		}()
	}

	wg.Wait()

	p := s.client.Pipeline()
	defer p.Finish()

	// Check for consistency
	for key, version := range s.kv {
		resp, err := p.MGet(key, MGetOptions{})()
		assert.Equal(t, nil, err)

		if resp.Type == MGetResponseTypeHD {
			panic("Invalid response type")
		}

		if resp.Type == MGetResponseTypeEN {
			continue
		}

		if resp.Flags&MGetFlagX > 0 {
			continue
		}

		list := strings.Split(string(resp.Data), ":")
		assert.Equal(t, key, list[0])

		num, err := strconv.ParseUint(list[1], 10, 64)
		assert.Equal(t, nil, err)
		assert.Equal(t, version, num)
	}
}

//revive:enable:cognitive-complexity

func TestMemcache_Consistent_Cache(t *testing.T) {
	client, err := New("localhost:11211", 2)
	assert.Equal(t, nil, err)

	for i := 0; i < 200; i++ {
		consistentCacheSingleLoop(t, client)
		if t.Failed() {
			fmt.Println("LOOP:", i)
			return
		}
	}

	err = client.Close()
	assert.Equal(t, nil, err)

	limiter := &client.conns[0].core.sender.selector.writeLimiter
	assert.Equal(t, limiter.cmdReadCount.Load(), limiter.cmdWriteCount)

	limiter = &client.conns[1].core.sender.selector.writeLimiter
	assert.Equal(t, limiter.cmdReadCount.Load(), limiter.cmdWriteCount)
}
