package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newCmdBuilder() *cmdBuilder {
	var b cmdBuilder
	initCmdBuilder(&b)
	return &b
}

func TestBuilder_AddMGet(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "mg some:key v\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMGet_With_N(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "mg some:key N13 v\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMGet_With_N_And_CAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13, CAS: true})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "mg some:key c N13 v\r\n", string(b.getCmd().requestData))

	freeCommandData(b.getCmd())

	// New Command Builder

	b = newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13, CAS: true})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "mg some:key c N13 v\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMSet(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "ms some:key 10\r\nSOME-VALUE\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMSet_WithCAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		CAS: 1234,
	})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "ms some:key 10 C1234\r\nSOME-VALUE\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMSet_With_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		TTL: 23,
	})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "ms some:key 10 T23\r\nSOME-VALUE\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMDel(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "md some:key\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMDel_With_CAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{CAS: 1234})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "md some:key C1234\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMDel_With_I(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{I: true})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "md some:key I\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMDel_With_CAS_And_I(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{CAS: 234, I: true})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "md some:key C234 I\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMDel_With_I_And_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{I: true, TTL: 23})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "md some:key I T23\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddMDel_With_Only_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{TTL: 23})

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "md some:key\r\n", string(b.getCmd().requestData))
}

func TestBuilder_AddFlushAll(t *testing.T) {
	b := newCmdBuilder()
	b.addFlushAll()

	assert.Equal(t, 1, b.getCmd().cmdCount)
	assert.Equal(t, "flush_all\r\n", string(b.getCmd().requestData))
}

func Benchmark_AddMGet(b *testing.B) {
	builder := newCmdBuilder()
	for n := 0; n < b.N; n++ {
		builder.addMGet("some:key", MGetOptions{N: 1234})
		builder.cmd.requestData = builder.cmd.requestData[:0]
	}
}
