package memcache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func pipelineFlushAll(p *Pipeline) {
	connFlushAll(p.c)
}

func assertMGetEqual(t *testing.T, a, b MGetResponse) {
	t.Helper()

	assert.Greater(t, b.CAS, uint64(0))
	b.CAS = 0
	assert.Equal(t, a, b)
}

func TestPipeline_Simple_MGet(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	resp, err := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, resp)
}

func TestPipeline_Multi_MGet(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	mg1 := p.MGet("key01", MGetOptions{N: 10, CAS: true})
	mg2 := p.MGet("key02", MGetOptions{N: 10, CAS: true})
	mg3 := p.MGet("key03", MGetOptions{N: 10, CAS: true})

	r1, err := mg1()
	assert.Equal(t, nil, err)
	r2, err := mg2()
	assert.Equal(t, nil, err)
	r3, err := mg3()
	assert.Equal(t, nil, err)

	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, r1)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, r2)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, r3)
}

func TestPipeline_MGet_Then_MSet_CAS(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	resp, err := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, resp)

	setResp, err := p.MSet("key01", []byte("simple\r\nvalue"), MSetOptions{CAS: resp.CAS})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, setResp)

	resp2, err := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("simple\r\nvalue"),
	}, resp2)
}

func TestPipeline_MGet_Then_MSet_Then_MDel(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	resp, _ := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	_, _ = p.MSet("key01", []byte("simple\r\nvalue"), MSetOptions{CAS: resp.CAS})()

	delResp, err := p.MDel("key01", MDelOptions{})()

	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{
		Type: MDelResponseTypeHD,
	}, delResp)

	resp, err = p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, resp)
}

func TestPipeline_MSet_Then_MDel_With_Invalidate_Then_MGet_Stale_Data(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	resp, _ := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	_, _ = p.MSet("key01", []byte("simple\r\nvalue"), MSetOptions{CAS: resp.CAS})()
	_, _ = p.MDel("key01", MDelOptions{I: true})()

	resp, err = p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte("simple\r\nvalue"),
		Flags: MGetFlagW | MGetFlagX,
	}, resp)
}

func TestPipeline_Multiple_Times(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	for k := 0; k < 10; k++ {
		resp, _ := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
		_, _ = p.MSet("key01", []byte(fmt.Sprintf("VALUES:%03d", k)), MSetOptions{CAS: resp.CAS})()
		_, _ = p.MDel("key01", MDelOptions{I: true})()
	}

	resp, err := p.MGet("key01", MGetOptions{N: 10, CAS: true})()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte("VALUES:009"),
		Flags: MGetFlagW | MGetFlagX,
	}, resp)
}

func TestPipeline_Simple_MGet_Call_Fn_Multi_Times(t *testing.T) {
	c, err := New("localhost:11211", 1, WithRetryDuration(5*time.Second))
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	fn := p.MGet("key01", MGetOptions{N: 10, CAS: true})

	resp, err := fn()
	assert.Equal(t, nil, err)
	assertMGetEqual(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte{},
		Flags: MGetFlagW,
	}, resp)

	resp, err = fn()
	assert.Equal(t, ErrAlreadyGotten, err)
}

func TestPipeline_Flush_All(t *testing.T) {
	c, err := New("localhost:11211", 1, WithRetryDuration(5*time.Second))
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	_, err = p.MSet("key01", []byte("some value"), MSetOptions{})()
	assert.Equal(t, nil, err)

	err = p.FlushAll()()
	assert.Equal(t, nil, err)

	resp, err := p.MGet("key01", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeEN,
	}, resp)
}

func Benchmark_Pipeline_Single_Thread(b *testing.B) {
	c, err := New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}

	connFlushAll(c.conns[0])

	for n := 0; n < b.N; n++ {
		func() {
			p := c.Pipeline()
			defer p.Finish()

			for i := 0; i < 100; i++ {
				resp, _ := p.MGet("campaign:key01", MGetOptions{N: 10, CAS: true})()
				_, _ = p.MSet("campaign:key01", []byte("some-random-string"), MSetOptions{CAS: resp.CAS})()
				_, _ = p.MDel("campaign:key01", MDelOptions{I: true})()
			}
		}()
	}
}

func Benchmark_Pipeline_Single_Thread_Batching_Set(b *testing.B) {
	c, err := New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}

	connFlushAll(c.conns[0])

	for n := 0; n < b.N; n++ {
		func() {
			p := c.Pipeline()
			defer p.Finish()

			for i := 0; i < 100; i++ {
				p.MSet("campaign:key01", []byte("some-random-string"), MSetOptions{})
			}
		}()
	}
}

func Benchmark_Pipeline_Single_Thread_Batching_Get(b *testing.B) {
	c, err := New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}

	const valueString = "some random string"

	root := c.Pipeline()
	pipelineFlushAll(root)
	_, _ = root.MSet("campaign:key01", []byte(valueString), MSetOptions{})()

	for n := 0; n < b.N; n++ {
		func() {
			p := c.Pipeline()
			defer p.Finish()

			for i := 0; i < 1000; i++ {
				p.MGet("campaign:key01", MGetOptions{N: 20, CAS: true})
			}
		}()
	}
}

func Benchmark_Pipeline_Multi_Threads(b *testing.B) {
	c, err := New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}

	const valueString = "some random string"
	const threadCount = 4

	root := c.Pipeline()
	pipelineFlushAll(root)
	_, _ = root.MSet("campaign:key01", []byte(valueString), MSetOptions{})()

	for n := 0; n < b.N; n++ {
		var wg sync.WaitGroup
		wg.Add(threadCount)

		for thread := 0; thread < threadCount; thread++ {
			go func() {
				defer wg.Done()

				const batchSize = 40
				const loopCount = 100000 / batchSize

				for loop := 0; loop < loopCount; loop++ {
					p := c.Pipeline()

					for i := 0; i < batchSize; i++ {
						p.MGet("campaign:key01", MGetOptions{N: 20, CAS: true})
					}

					p.Finish()
				}
			}()
		}

		wg.Wait()
	}
}
