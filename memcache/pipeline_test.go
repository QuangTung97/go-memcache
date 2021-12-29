package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
