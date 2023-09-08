package memcache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newCmdBuilder() *cmdBuilder {
	var b cmdBuilder
	initCmdBuilder(&b, 1000)
	return &b
}

func TestBuilder_AddMGet(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key v\r\n", string(cmd.requestData))
	assert.Equal(t, 1, cap(cmd.responseBinaries))
}

func TestBuilder_AddMGet_With_N(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key N13 v\r\n", string(cmd.requestData))
	assert.Equal(t, 1, cap(cmd.responseBinaries))
}

func TestBuilder_AddMGet_And_MSet_Multi_Times(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key01", MGetOptions{N: 13})
	b.addMGet("some:key02", MGetOptions{N: 14})
	b.addMSet("key03", []byte("data01"), MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 3, cmd.cmdCount)
	assert.Equal(t,
		"mg some:key01 N13 v\r\nmg some:key02 N14 v\r\nms key03 6\r\ndata01\r\n",
		string(cmd.requestData),
	)
	assert.Equal(t, 2, cap(cmd.responseBinaries))

	initCmdBuilder(b, 1000)
	assert.Equal(t, 0, cap(b.getCurrentCommandForTest().responseBinaries))
}

func TestBuilder_AddMGet_With_N_And_CAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13, CAS: true})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key c N13 v\r\n", string(cmd.requestData))
	assert.Equal(t, 1, cap(cmd.responseBinaries))

	freeCommandResponseData(cmd)

	// New Command Builder

	b = newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13, CAS: true})
	cmd = b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key c N13 v\r\n", string(cmd.requestData))
	assert.Equal(t, 1, cap(cmd.responseBinaries))
}

func TestBuilder_AddMSet(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10\r\nSOME-VALUE\r\n", string(cmd.requestData))
}

func TestBuilder_AddMSet_Length_Zero(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", nil, MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 0\r\n\r\n", string(cmd.requestData))
	assert.Equal(t, 0, cap(cmd.responseBinaries))
}

func TestBuilder_AddMSet_WithCAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		CAS: 1234,
	})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10 C1234\r\nSOME-VALUE\r\n", string(cmd.requestData))
	assert.Equal(t, 0, cap(cmd.responseBinaries))
}

func TestBuilder_AddMSet_With_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		TTL: 23,
	})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10 T23\r\nSOME-VALUE\r\n", string(cmd.requestData))
}

func TestBuilder_AddMDel(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "md some:key\r\n", string(cmd.requestData))
}

func TestBuilder_AddMDel_With_CAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{CAS: 1234})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "md some:key C1234\r\n", string(cmd.requestData))
}

func TestBuilder_AddMDel_With_I(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{I: true})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "md some:key I\r\n", string(cmd.requestData))
}

func TestBuilder_AddMDel_With_CAS_And_I(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{CAS: 234, I: true})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "md some:key C234 I\r\n", string(cmd.requestData))
}

func TestBuilder_AddMDel_With_I_And_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{I: true, TTL: 23})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "md some:key I T23\r\n", string(cmd.requestData))
}

func TestBuilder_AddMDel_With_Only_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMDel("some:key", MDelOptions{TTL: 23})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "md some:key\r\n", string(cmd.requestData))
}

func TestBuilder_AddFlushAll(t *testing.T) {
	b := newCmdBuilder()
	b.addFlushAll()
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "flush_all\r\n", string(cmd.requestData))
}

func Benchmark_AddMGet(b *testing.B) {
	builder := newCmdBuilder()
	for n := 0; n < b.N; n++ {
		builder.addMGet("some:key", MGetOptions{N: 1234})

		cmd := builder.getCurrentCommandForTest()
		cmd.requestData = cmd.requestData[:0]
	}
}

func TestBuilder_AddMGet_Then_Clear(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key v\r\n", string(cmd.requestData))
	assert.Equal(t, 1, cap(cmd.responseBinaries))

	b.clearCmd()

	assert.Nil(t, b.getCurrentCommandForTest())
}

func newCmdBuilderWithMax(maxCount int) *cmdBuilder {
	var b cmdBuilder
	initCmdBuilder(&b, maxCount)
	return &b
}

func TestBuilder_With_Max_Count(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		b := newCmdBuilderWithMax(2)

		b.addMGet("key01", MGetOptions{})
		b.addMSet("key02", []byte("data 01"), MSetOptions{})

		b.addMGet("key03", MGetOptions{})
		b.addMGet("key04", MGetOptions{})

		b.addMDel("key05", MDelOptions{})

		cmd := b.finish()

		assert.Same(t, cmd, b.getCommandList())

		assert.Equal(t, 2, cmd.cmdCount)
		assert.Equal(t, "mg key01 v\r\nms key02 7\r\ndata 01\r\n", string(cmd.requestData))
		assert.Equal(t, 1, cap(cmd.responseBinaries))

		cmd = cmd.sibling
		assert.Equal(t, 2, cmd.cmdCount)
		assert.Equal(t, "mg key03 v\r\nmg key04 v\r\n", string(cmd.requestData))
		assert.Equal(t, 2, cap(cmd.responseBinaries))

		cmd = cmd.sibling
		assert.Equal(t, 1, cmd.cmdCount)
		assert.Equal(t, "md key05\r\n", string(cmd.requestData))
		assert.Equal(t, 0, cap(cmd.responseBinaries))

		assert.Nil(t, cmd.sibling)
	})
}
