package memcache

import (
	"math/bits"
	"sync"
	"unsafe"
)

const (
	poolGetDataBaseSizeLog = 6
	poolGetDataBaseSize    = 1 << poolGetDataBaseSizeLog
	poolNumLevels          = 12
	poolMaxBytes           = poolGetDataBaseSize * (1 << (poolNumLevels - 1))
)

func initGetDataPools() []sync.Pool {
	result := make([]sync.Pool, poolNumLevels)
	for i := range result {
		level := i
		result[i].New = func() any {
			data := make([]byte, 1<<(poolGetDataBaseSizeLog+level))
			return &data[0]
		}
	}
	return result
}

var getDataPools = initGetDataPools()

func getByteSlice(size uint64) []byte {
	if size == 0 {
		return make([]byte, 0)
	}
	if size > poolMaxBytes {
		return make([]byte, size)
	}

	numBits := bits.Len32(uint32(size - 1))
	level := numBits - poolGetDataBaseSizeLog
	if numBits < poolGetDataBaseSizeLog {
		level = 0
	}

	dataPtr := getDataPools[level].Get().(*byte)
	data := unsafe.Slice(dataPtr, 1<<(level+poolGetDataBaseSizeLog))

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
	if numBits < poolGetDataBaseSizeLog {
		return
	}
	level := numBits - poolGetDataBaseSizeLog

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

func newPipelineCmdFromPool() *pipelineCmd {
	return pipelineCmdPool.Get().(*pipelineCmd)
}

func putPipelineCmdToPool(cmd *pipelineCmd) {
	*cmd = pipelineCmd{}
	pipelineCmdPool.Put(cmd)
}
