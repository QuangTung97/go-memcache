package memcache

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestPoolMaxSize(t *testing.T) {
	assert.Equal(t, 64, poolBytesBaseSize)
	assert.Equal(t, 128*1024, poolMaxBytes)
}

func TestPool_GetByteSlice(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		x := getByteSlice(64)
		assert.Equal(t, 64, len(x))
		assert.Equal(t, 64, cap(x))
	})

	t.Run("size < 64", func(t *testing.T) {
		x := getByteSlice(63)
		assert.Equal(t, 63, len(x))
		assert.Equal(t, 64, cap(x))

		x = getByteSlice(65)
		assert.Equal(t, 65, len(x))
		assert.Equal(t, 128, cap(x))

		x = getByteSlice(127)
		assert.Equal(t, 127, len(x))
		assert.Equal(t, 128, cap(x))

		x = getByteSlice(127)
		assert.Equal(t, 127, len(x))
		assert.Equal(t, 128, cap(x))

		x = getByteSlice(256)
		assert.Equal(t, 256, len(x))
		assert.Equal(t, 256, cap(x))
	})

	t.Run("zero", func(t *testing.T) {
		x := getByteSlice(0)
		assert.Equal(t, 0, len(x))
		assert.Equal(t, 0, cap(x))

		x = getByteSlice(1)
		assert.Equal(t, 1, len(x))
		assert.Equal(t, 64, cap(x))
	})

	t.Run("bigger than max", func(t *testing.T) {
		x := getByteSlice(128*1024 + 1)
		assert.Equal(t, 128*1024+1, len(x))
		assert.Equal(t, 128*1024+1, cap(x))
	})

	t.Run("get and put and get", func(t *testing.T) {
		x := getByteSlice(128)
		ReleaseGetResponseData(x)

		x = getByteSlice(65)
		assert.Equal(t, 65, len(x))
		assert.Equal(t, 128, cap(x))
	})

	t.Run("release nil", func(t *testing.T) {
		ReleaseGetResponseData(nil)

		x := getByteSlice(1)
		assert.Equal(t, 1, len(x))
		assert.Equal(t, 64, cap(x))
	})

	t.Run("release bigger than max", func(t *testing.T) {
		ReleaseGetResponseData(make([]byte, 128*1024+1))

		x := getByteSlice(128*1024 - 1)
		assert.Equal(t, 128*1024-1, len(x))
		assert.Equal(t, 128*1024, cap(x))
	})

	t.Run("release not align", func(t *testing.T) {
		ReleaseGetResponseData(make([]byte, 63))

		x := getByteSlice(63)
		assert.Equal(t, 63, len(x))
		assert.Equal(t, 64, cap(x))
	})

	t.Run("release too small", func(t *testing.T) {
		ReleaseGetResponseData(make([]byte, 32))

		x := getByteSlice(32)
		assert.Equal(t, 32, len(x))
		assert.Equal(t, 64, cap(x))
	})

	t.Run("release len zero", func(t *testing.T) {
		ReleaseGetResponseData(make([]byte, 0, 64))

		x := getByteSlice(32)
		assert.Equal(t, 32, len(x))
		assert.Equal(t, 64, cap(x))
	})
}

func BenchmarkPool_Get_And_Put(b *testing.B) {
	sum := 0
	for n := 0; n < b.N; n++ {
		x := getByteSlice(1024 - 1)
		sum += len(x)
		ReleaseGetResponseData(x)
	}
}

var pipelineCmdPointer unsafe.Pointer

func BenchmarkPipelineCmd_New(b *testing.B) {
	for n := 0; n < b.N; n++ {
		cmd := &pipelineCmd{}
		atomic.StorePointer(&pipelineCmdPointer, unsafe.Pointer(cmd))
	}
}

func TestPipelineCmdPool(t *testing.T) {
	cmd := getPipelineCmdFromPool()
	cmd.cmdType = commandTypeMGet
	putPipelineCmdToPool(cmd)

	cmd = getPipelineCmdFromPool()
	assert.Equal(t, &pipelineCmd{}, cmd)
}

func BenchmarkPipelineCmd_Pool(b *testing.B) {
	for n := 0; n < b.N; n++ {
		cmd := getPipelineCmdFromPool()
		atomic.StorePointer(&pipelineCmdPointer, unsafe.Pointer(cmd))
		putPipelineCmdToPool(cmd)
	}
}

func TestPipelineCommandListPool(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		p := newPipelineCommandListPool()

		cmdList := p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		assert.Equal(t, 100, cap(cmdList))

		p.putCommandList(cmdList)

		cmdList = p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		assert.Equal(t, 100, cap(cmdList))
	})

	t.Run("normal", func(t *testing.T) {
		p := newPipelineCommandListPool()

		cmdList := p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		assert.Equal(t, 100, cap(cmdList))

		cmdList = append(cmdList, &pipelineCmd{})
		cmdList = append(cmdList, &pipelineCmd{})

		p.putCommandList(cmdList)

		cmdList = p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		cmdList = cmdList[:2]
		assert.Nil(t, cmdList[0])
		assert.Nil(t, cmdList[1])
	})

	t.Run("put random command list", func(t *testing.T) {
		p := newPipelineCommandListPool()

		p.putCommandList(make([]*pipelineCmd, 13))

		cmdList := p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		assert.Equal(t, 100, cap(cmdList))

		cmdList = cmdList[:100]
		assert.Nil(t, cmdList[99])
	})

	t.Run("other max len", func(t *testing.T) {
		p := newPipelineCommandListPool(WithMaxCommandsPerBatch(120))

		cmdList := p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		assert.Equal(t, 120, cap(cmdList))

		p.putCommandList(make([]*pipelineCmd, 100))

		cmdList = p.getCommandList()
		assert.Equal(t, 0, len(cmdList))
		assert.Equal(t, 120, cap(cmdList))
	})
}
