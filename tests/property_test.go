package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache"
	"hash/crc32"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type propertyTest struct {
	totalNotFound atomic.Uint64
	totalFound    atomic.Uint64

	totalSetSuccess atomic.Uint64
	totalServerErr  atomic.Uint64

	totalDelete atomic.Uint64

	client *memcache.Client
}

func newPropertyTest(t *testing.T) *propertyTest {
	client, err := memcache.New("localhost:11211", 3)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pipe := client.Pipeline()
	t.Cleanup(pipe.Finish)

	err = pipe.FlushAll()()
	if err != nil {
		panic(err)
	}

	return &propertyTest{
		client: client,
	}
}

func randInt(r *rand.Rand, a, b int) int {
	return r.Intn(b-a+1) + a
}

const numLoops = 10000
const keySpace = 789

func randomKey(r *rand.Rand) string {
	n := randInt(r, 1, keySpace)
	return fmt.Sprintf("KEY%06d", n)
}

func randomValue(r *rand.Rand, key string) []byte {
	keyEnd := 4 + len(key)
	n := randInt(r, keyEnd, 1<<10)

	data := make([]byte, n)

	fill := byte(randInt(r, 0, 255))
	for i := range data {
		data[i] = fill
	}

	copy(data[4:keyEnd], key)

	sum := crc32.ChecksumIEEE(data[4:])
	binary.LittleEndian.PutUint32(data[:4], sum)

	return data
}

func (p *propertyTest) doGetSingleLoop(r *rand.Rand) {
	n := randInt(r, 1, 10)

	pipe := p.client.Pipeline()
	defer pipe.Finish()

	keys := make([]string, 0, n)
	fnList := make([]func() (memcache.MGetResponse, error), 0, n)

	for i := 0; i < n; i++ {
		k := randomKey(r)
		keys = append(keys, k)
		fn := pipe.MGet(k, memcache.MGetOptions{CAS: true})
		fnList = append(fnList, fn)
	}

	for index, fn := range fnList {
		key := keys[index]

		resp, err := fn()
		if err != nil {
			panic(err)
		}

		if resp.Type == memcache.MGetResponseTypeEN {
			p.totalNotFound.Add(1)
			continue
		}

		if resp.Type == memcache.MGetResponseTypeVA {
			p.totalFound.Add(1)

			data := resp.Data

			sum := crc32.ChecksumIEEE(data[4:])

			expectedPrefix := make([]byte, 4)
			binary.LittleEndian.PutUint32(expectedPrefix, sum)

			if !bytes.Equal(data[:4], expectedPrefix) {
				panic("Mismatch crc check")
			}

			if !bytes.Equal(data[4:4+len(key)], []byte(key)) {
				panic("Mismatch key")
			}

			continue
		}

		panic("Invalid resp type: " + fmt.Sprint(resp.Type))
	}
}

func (p *propertyTest) doGet(r *rand.Rand) {
	for i := 0; i < numLoops; i++ {
		p.doGetSingleLoop(r)
	}
}

func (p *propertyTest) doSet(r *rand.Rand) {
	for i := 0; i < numLoops; i++ {
		p.doSetSingleLoop(r)
	}
}

func (p *propertyTest) doDelete(r *rand.Rand) {
	for i := 0; i < numLoops; i++ {
		p.doDeleteSingleLoop(r)
	}
}

func (p *propertyTest) doSetSingleLoop(r *rand.Rand) {
	n := randInt(r, 1, 10)

	pipe := p.client.Pipeline()
	defer pipe.Finish()

	fnList := make([]func() (memcache.MSetResponse, error), 0, n)

	for i := 0; i < n; i++ {
		k := randomKey(r)
		data := randomValue(r, k)

		fn := pipe.MSet(k, data, memcache.MSetOptions{})
		fnList = append(fnList, fn)
	}

	for _, fn := range fnList {
		_, err := fn()
		if err == nil {
			p.totalSetSuccess.Add(1)
			continue
		}

		if memcache.IsServerError(err) {
			serverErr := err.(memcache.ErrServerError)
			fmt.Println("SERVER ERROR:", serverErr.Message)

			p.totalServerErr.Add(1)
			continue
		}

		panic(err)
	}
}

func (p *propertyTest) doDeleteSingleLoop(r *rand.Rand) {
	n := randInt(r, 1, 4)

	pipe := p.client.Pipeline()
	defer pipe.Finish()

	fnList := make([]func() (memcache.MDelResponse, error), 0, n)

	for i := 0; i < n; i++ {
		k := randomKey(r)

		fn := pipe.MDel(k, memcache.MDelOptions{})
		fnList = append(fnList, fn)
	}

	for _, fn := range fnList {
		_, err := fn()
		if err == nil {
			p.totalDelete.Add(1)
			continue
		}

		panic(err)
	}
}

func TestPropertyBased(t *testing.T) {
	p := newPropertyTest(t)

	seedValue := time.Now().UnixNano()
	fmt.Println("SEED:", seedValue)

	var seed atomic.Int64
	seed.Add(seedValue)

	newRand := func() *rand.Rand {
		newSeed := seed.Add(1)
		return rand.New(rand.NewSource(newSeed))
	}

	const getThreads = 20
	const setThreads = 10
	const deleteThreads = 4

	var wg sync.WaitGroup
	wg.Add(getThreads + setThreads + deleteThreads)

	for th := 0; th < getThreads; th++ {
		go func() {
			defer wg.Done()
			p.doGet(newRand())
		}()
	}

	for th := 0; th < setThreads; th++ {
		go func() {
			defer wg.Done()
			p.doSet(newRand())
		}()
	}

	for th := 0; th < deleteThreads; th++ {
		go func() {
			defer wg.Done()
			p.doDelete(newRand())
		}()
	}

	wg.Wait()

	fmt.Println("TOTAL FOUND:", p.totalFound.Load())
	fmt.Println("TOTAL NOT FOUND:", p.totalNotFound.Load())
	fmt.Println("TOTAL SET SUCCESS:", p.totalSetSuccess.Load())
	fmt.Println("TOTAL SERVER ERROR:", p.totalServerErr.Load())
	fmt.Println("TOTAL DELETE:", p.totalDelete.Load())
}
