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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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

func TestPipeline_Execute(t *testing.T) {
	c, err := New("localhost:11211", 1, WithRetryDuration(5*time.Second))
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	p.MSet("key01", []byte("some value 01"), MSetOptions{})
	p.MSet("key02", []byte("some value 02"), MSetOptions{})
	p.Execute()

	another := c.Pipeline()
	defer another.Finish()

	resp, err := another.MGet("key01", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("some value 01"),
	}, resp)

	resp, err = another.MGet("key02", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("some value 02"),
	}, resp)
}

func TestPipeline_Execute_And_Get_On_The_Same_Pipeline(t *testing.T) {
	c, err := New("localhost:11211", 1, WithRetryDuration(5*time.Second))
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	p.MSet("key01", []byte("some value 01"), MSetOptions{})
	p.MSet("key02", []byte("some value 02"), MSetOptions{})
	p.Execute()

	p.MSet("key03", []byte("some value 03"), MSetOptions{})

	resp, err := p.MGet("key01", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("some value 01"),
	}, resp)

	resp, err = p.MGet("key03", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("some value 03"),
	}, resp)
}

func TestPipeline_MSet_MGet_MDel__Invalid_Key_Format(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	const key = "abcd\x00"

	resp, err := p.MSet(key, []byte("Hello"), MSetOptions{})()
	assert.Equal(t, ErrInvalidKeyFormat, err)
	assert.Equal(t, MSetResponse{}, resp)

	getResp, err := p.MGet(key, MGetOptions{})()
	assert.Equal(t, ErrInvalidKeyFormat, err)
	assert.Equal(t, MGetResponse{}, getResp)

	delResp, err := p.MDel(key, MDelOptions{})()
	assert.Equal(t, ErrInvalidKeyFormat, err)
	assert.Equal(t, MDelResponse{}, delResp)

	// Set Another Keys
	setResp, err := p.MSet("key01", []byte("value01"), MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeHD,
	}, setResp)

	setResp, err = p.MSet("key02", []byte("value02"), MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeHD,
	}, setResp)

	// Get Keys
	fn1 := p.MGet("key01", MGetOptions{})
	fn2 := p.MGet("key02", MGetOptions{})

	getResp, err = fn1()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("value01"),
	}, getResp)

	getResp, err = fn2()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("value02"),
	}, getResp)
}

func TestPipeline_MSet_With_Response_Type_EXISTS_And_NOT_FOUND(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	setResp, err := p.MSet("key01", []byte("value01"), MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeHD,
	}, setResp)

	getResp, err := p.MGet("key01", MGetOptions{CAS: true})()
	assert.Equal(t, nil, err)

	cas := getResp.CAS
	getResp.CAS = 100
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("value01"),
		CAS:  100,
	}, getResp)
	assert.Greater(t, cas, uint64(0))

	setResp, err = p.MSet("key01", []byte("value02"), MSetOptions{CAS: cas - 1})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeEX}, setResp)

	setResp, err = p.MSet("key01", []byte("value03"), MSetOptions{CAS: cas})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, setResp)

	// Get Again
	getResp, err = p.MGet("key01", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("value03"),
	}, getResp)

	// Set With Not Existed Key
	setResp, err = p.MSet("key02", []byte("value04"), MSetOptions{CAS: 123})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeNF}, setResp)
}

func TestPipeline_MDel__Not_Found_And_Exists(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	_, err = p.MSet("key01", []byte("value01"), MSetOptions{})()
	assert.Equal(t, nil, err)

	delResp, err := p.MDel("key01", MDelOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{Type: MDelResponseTypeHD}, delResp)

	delResp, err = p.MDel("key01", MDelOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{Type: MDelResponseTypeNF}, delResp)

	_, err = p.MSet("key01", []byte("value02"), MSetOptions{})()
	assert.Equal(t, nil, err)

	getResp, err := p.MGet("key01", MGetOptions{CAS: true})()
	assert.Equal(t, nil, err)

	delResp, err = p.MDel("key01", MDelOptions{CAS: getResp.CAS - 1})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{Type: MDelResponseTypeEX}, delResp)

	// Get Again
	getResp, err = p.MGet("key01", MGetOptions{CAS: true})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponseTypeVA, getResp.Type)

	delResp, err = p.MDel("key01", MDelOptions{CAS: getResp.CAS})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{Type: MDelResponseTypeHD}, delResp)
}

func repeatBytes(c byte, n int) []byte {
	result := make([]byte, n)
	for i := range result {
		result[i] = c
	}
	return result
}

func TestPipeline_MSet_Data__TOO_BIG(t *testing.T) {
	c, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	const headerSize = 56 // 8 * 7
	const key = "key01"
	const paddingSize = 1 + 2 // 1 null terminated key, 2 for \r\n data

	const maxDataSize = 1024*1024 - headerSize - len(key) - paddingSize

	setResp, err := p.MSet(key, repeatBytes('A', maxDataSize+1), MSetOptions{})()
	assert.Equal(t, NewServerError("object too large for cache"), err)
	assert.Equal(t, MSetResponse{}, setResp)

	setResp, err = p.MSet(key, repeatBytes('A', maxDataSize), MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, setResp)

	getResp, err := p.MGet(key, MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: repeatBytes('A', maxDataSize),
	}, getResp)
}

func TestPipeline_Not_Blocking__When_Wait_For_Response__After_Close(t *testing.T) {
	for i := 0; i < 4000; i++ {
		func() {
			c, err := New("localhost:11211", 1)
			assert.Equal(t, nil, err)
			defer func() { _ = c.Close() }()

			p := c.Pipeline()
			defer p.Finish()

			pipelineFlushAll(p)

			go func() {
				_ = c.Close()
			}()

			for k := 0; k < 100; k++ {
				fn1 := p.MSet("key01", []byte("value01"), MSetOptions{})
				fn2 := p.MGet("key01", MGetOptions{})

				_, _ = fn1()
				_, _ = fn2()
			}
		}()
	}
}

func TestValidateKeyFormat(t *testing.T) {
	err := validateKeyFormat("\x00")
	assert.Equal(t, ErrInvalidKeyFormat, err)

	err = validateKeyFormat("\r\n")
	assert.Equal(t, ErrInvalidKeyFormat, err)

	err = validateKeyFormat("\t")
	assert.Equal(t, ErrInvalidKeyFormat, err)
}

func Benchmark_Pipeline_Single_Thread(b *testing.B) {
	c, err := New("localhost:11211", 1)
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
	defer func() { _ = c.Close() }()

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
