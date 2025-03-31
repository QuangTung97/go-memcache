package memcache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (b *cmdBuilder) getCurrentCommandForTest() *commandListData {
	return b.cmd
}

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
	assert.Equal(t, 4, cap(cmd.responseBinaries))
}

func TestBuilder_AddMGet_With_N(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key N13 v\r\n", string(cmd.requestData))
	assert.Equal(t, 4, cap(cmd.responseBinaries))
}

func TestBuilder_AddMGet_And_MSet_Multi_Times(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key01", MGetOptions{N: 13})
	b.addMGet("some:key02", MGetOptions{N: 14})
	b.addMSet("key03", []byte("data01"), MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 3, cmd.cmdCount)
	assert.Equal(t,
		"mg some:key01 N13 v\r\nmg some:key02 N14 v\r\nms key03 6\r\n",
		string(cmd.requestData),
	)
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len(cmd.requestData),
			data:   []byte("data01\r\n"),
		},
	}, traverseRequestBinaries(cmd))
	assert.Equal(t, 4, cap(cmd.responseBinaries))

	initCmdBuilder(b, 1000)
	assert.Equal(t, 0, cap(b.getCurrentCommandForTest().responseBinaries))
}

func TestBuilder_AddMGet_With_N_And_CAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13, CAS: true})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key c N13 v\r\n", string(cmd.requestData))
	assert.Equal(t, 4, cap(cmd.responseBinaries))

	freeCommandResponseData(cmd)

	// New Command Builder

	b = newCmdBuilder()
	b.addMGet("some:key", MGetOptions{N: 13, CAS: true})
	cmd = b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "mg some:key c N13 v\r\n", string(cmd.requestData))
	assert.Equal(t, 4, cap(cmd.responseBinaries))
}

func traverseRequestBinaries(cmd *commandListData) []requestBinaryEntry {
	result := make([]requestBinaryEntry, 0)
	for current := cmd.requestBinaries; current != nil; current = current.next {
		e := *current
		e.next = nil
		result = append(result, e)
	}
	return result
}

func TestBuilder_AddMSet(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len("ms some:key 10\r\n"),
			data:   []byte("SOME-VALUE\r\n"),
		},
	}, traverseRequestBinaries(cmd))
}

func TestBuilder_AddMSet_Length_Zero(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", nil, MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 0\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len(cmd.requestData),
			data:   []byte("\r\n"),
		},
	}, traverseRequestBinaries(cmd))
	assert.Equal(t, 0, cap(cmd.responseBinaries))
}

func TestBuilder_AddMSet_WithCAS(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		CAS: 1234,
	})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10 C1234\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len(cmd.requestData),
			data:   []byte("SOME-VALUE\r\n"),
		},
	}, traverseRequestBinaries(cmd))
	assert.Equal(t, 0, cap(cmd.responseBinaries))
}

func TestBuilder_AddMSet_With_TTL(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		TTL: 23,
	})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10 T23\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len(cmd.requestData),
			data:   []byte("SOME-VALUE\r\n"),
		},
	}, traverseRequestBinaries(cmd))
}

func TestBuilder_AddMSet_Multi(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		TTL: 23,
	})
	b.addMSet("key02", []byte("VALUE02"), MSetOptions{
		CAS: 88,
	})
	cmd := b.finish()

	assert.Equal(t, 2, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10 T23\r\nms key02 7 C88\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len("ms some:key 10 T23\r\n"),
			data:   []byte("SOME-VALUE\r\n"),
		},
		{
			offset: len(cmd.requestData),
			data:   []byte("VALUE02\r\n"),
		},
	}, traverseRequestBinaries(cmd))
}

func TestBuilder_AddMSet_Multi__Exceed_Max_Count(t *testing.T) {
	b := newCmdBuilderWithMax(2)
	b.addMSet("some:key", []byte("SOME-VALUE"), MSetOptions{
		TTL: 23,
	})
	b.addMSet("key02", []byte("VALUE02"), MSetOptions{
		CAS: 88,
	})
	b.addMSet("key03", []byte("VALUE03"), MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 2, cmd.cmdCount)
	assert.Equal(t, "ms some:key 10 T23\r\nms key02 7 C88\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len("ms some:key 10 T23\r\n"),
			data:   []byte("SOME-VALUE\r\n"),
		},
		{
			offset: len(cmd.requestData),
			data:   []byte("VALUE02\r\n"),
		},
	}, traverseRequestBinaries(cmd))

	cmd = cmd.sibling
	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms key03 7\r\n", string(cmd.requestData))
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len(cmd.requestData),
			data:   []byte("VALUE03\r\n"),
		},
	}, traverseRequestBinaries(cmd))

	assert.Nil(t, cmd.sibling)
}

func TestBuilder_AddMSet__Check_Request_Binary_Capacity(t *testing.T) {
	b := newCmdBuilderWithMax(2)

	data := []byte(strings.Repeat("A", 125))
	b.addMSet("key01", data, MSetOptions{})

	data = append(data, 'B', 'C')
	b.addMSet("key02", data, MSetOptions{})

	cmd := b.finish()

	assert.Equal(t, 2, cmd.cmdCount)
	assert.Equal(t, "ms key01 125\r\nms key02 127\r\n", string(cmd.requestData))

	entries := traverseRequestBinaries(cmd)
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len("ms key01 126\r\n"),
			data:   []byte(strings.Repeat("A", 125) + "\r\n"),
		},
		{
			offset: len(cmd.requestData),
			data:   []byte(strings.Repeat("A", 125) + "BC\r\n"),
		},
	}, entries)

	assert.Equal(t, 128, cap(entries[0].data))
	assert.Equal(t, 256, cap(entries[1].data))
}

func TestBuilder_AddMSet__Check_Request_Binary_Capacity__At_Boundary(t *testing.T) {
	b := newCmdBuilderWithMax(2)

	data := []byte(strings.Repeat("A", 126))
	b.addMSet("key01", data, MSetOptions{})

	data = append(data, 'B')
	b.addMSet("key02", data, MSetOptions{})

	cmd := b.finish()

	assert.Equal(t, 2, cmd.cmdCount)
	assert.Equal(t, "ms key01 126\r\nms key02 127\r\n", string(cmd.requestData))

	entries := traverseRequestBinaries(cmd)
	assert.Equal(t, []requestBinaryEntry{
		{
			offset: len("ms key01 126\r\n"),
			data:   []byte(strings.Repeat("A", 126) + "\r\n"),
		},
		{
			offset: len(cmd.requestData),
			data:   []byte(strings.Repeat("A", 126) + "B\r\n"),
		},
	}, entries)

	assert.Equal(t, 128, cap(entries[0].data))
	assert.Equal(t, 256, cap(entries[1].data))
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

func TestBuilder_AddVersion(t *testing.T) {
	b := newCmdBuilder()
	b.addVersion()
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "version\r\n", string(cmd.requestData))
}

func TestBuilder_AddVersion_Multi_Times(t *testing.T) {
	b := newCmdBuilder()
	b.addVersion()
	b.addVersion()
	cmd := b.finish()

	assert.Equal(t, 2, cmd.cmdCount)
	assert.Equal(t, "version\r\nversion\r\n", string(cmd.requestData))
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
	assert.Equal(t, 4, cap(cmd.responseBinaries))

	b.clearCmd()

	assert.Nil(t, b.getCurrentCommandForTest())
}

func TestBuilder_AddMSet_Then_Clear(t *testing.T) {
	b := newCmdBuilder()
	b.addMSet("some:key", []byte("some data"), MSetOptions{})
	cmd := b.finish()

	assert.Equal(t, 1, cmd.cmdCount)
	assert.Equal(t, "ms some:key 9\r\n", string(cmd.requestData))
	assert.Equal(t, 1, len(traverseRequestBinaries(cmd)))
	assert.Equal(t, 0, cap(cmd.responseBinaries))

	b.clearCmd()

	assert.Nil(t, b.getCurrentCommandForTest())
	assert.Nil(t, b.cmdList)
	assert.Nil(t, b.lastPointer)
	assert.Nil(t, b.lastRequestEntry)
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
		assert.Equal(t, "mg key01 v\r\nms key02 7\r\n", string(cmd.requestData))
		assert.Equal(t, 4, cap(cmd.responseBinaries))
		assert.Equal(t, []requestBinaryEntry{
			{
				offset: len("mg key01 v\r\nms key02 7\r\n"),
				data:   []byte("data 01\r\n"),
			},
		}, traverseRequestBinaries(cmd))

		cmd = cmd.sibling
		assert.Equal(t, 2, cmd.cmdCount)
		assert.Equal(t, "mg key03 v\r\nmg key04 v\r\n", string(cmd.requestData))
		assert.Equal(t, 4, cap(cmd.responseBinaries))

		cmd = cmd.sibling
		assert.Equal(t, 1, cmd.cmdCount)
		assert.Equal(t, "md key05\r\n", string(cmd.requestData))
		assert.Equal(t, 0, cap(cmd.responseBinaries))

		assert.Nil(t, cmd.sibling)
	})
}
