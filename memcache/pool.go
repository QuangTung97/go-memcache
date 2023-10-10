package memcache

import (
	"math/bits"
	"sync"
	"unsafe"
)

const (
	poolBytesBaseSizeLog = 6
	poolBytesBaseSize    = 1 << poolBytesBaseSizeLog
	poolBytesNumLevels   = 12
	poolMaxBytes         = poolBytesBaseSize * (1 << (poolBytesNumLevels - 1))
)

func initGetDataPools() []sync.Pool {
	result := make([]sync.Pool, poolBytesNumLevels)
	for i := range result {
		level := i
		result[i].New = func() any {
			data := make([]byte, 1<<(poolBytesBaseSizeLog+level))
			return &data[0]
		}
	}
	return result
}

var getDataPools = initGetDataPools()

func getByteSlice(size uint64) []byte {
	if size == 0 {
		return nil
	}
	if size > poolMaxBytes {
		return make([]byte, size)
	}

	numBits := bits.Len32(uint32(size - 1))
	level := numBits - poolBytesBaseSizeLog
	if numBits < poolBytesBaseSizeLog {
		level = 0
	}

	dataPtr := getDataPools[level].Get().(*byte)
	data := unsafe.Slice(dataPtr, 1<<(level+poolBytesBaseSizeLog))

	return data[:size]
}

// ReleaseGetResponseData store response data for reuse
func ReleaseGetResponseData(data []byte) {
	capacity := cap(data)
	if capacity == 0 {
		return
	}
	if capacity > poolMaxBytes {
		return
	}

	numBits := bits.Len32(uint32(capacity - 1))
	if capacity != (1 << numBits) {
		return
	}
	if numBits < poolBytesBaseSizeLog {
		return
	}
	level := numBits - poolBytesBaseSizeLog

	data = data[:1]
	getDataPools[level].Put(&data[0])
}

// ======================================
// Pipeline Command Pool
// ======================================
var pipelineCmdPool = sync.Pool{
	New: func() any {
		return &pipelineCmd{}
	},
}

func getPipelineCmdFromPool() *pipelineCmd {
	return pipelineCmdPool.Get().(*pipelineCmd)
}

func putPipelineCmdToPool(cmd *pipelineCmd) {
	*cmd = pipelineCmd{}
	pipelineCmdPool.Put(cmd)
}

// ======================================
// Pipeline Command List Pool
// ======================================
type pipelineCommandListPool struct {
	listSize int
	pool     sync.Pool
}

func newPipelineCommandListPool(options ...Option) *pipelineCommandListPool {
	opts := computeOptions(options...)
	listSize := opts.maxCommandsPerBatch

	return &pipelineCommandListPool{
		listSize: listSize,
		pool: sync.Pool{
			New: func() any {
				data := make([]*pipelineCmd, listSize)
				return &data[0]
			},
		},
	}
}

func (p *pipelineCommandListPool) getCommandList() []*pipelineCmd {
	dataPtr := p.pool.Get().(**pipelineCmd)
	data := unsafe.Slice(dataPtr, p.listSize)
	return data[:0]
}

func (p *pipelineCommandListPool) putCommandList(cmdList []*pipelineCmd) {
	if cap(cmdList) != p.listSize {
		return
	}

	for i := range cmdList {
		cmdList[i] = nil
	}

	data := cmdList[:1]
	p.pool.Put(&data[0])
}
