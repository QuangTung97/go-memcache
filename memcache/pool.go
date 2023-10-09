package memcache

import (
	"math/bits"
	"sync"
)

const (
	poolGetDataBaseSizeLog = 6
	poolGetDataBaseSize    = 1 << poolGetDataBaseSizeLog
	poolNumLevels          = 12
	poolMaxBytes           = poolGetDataBaseSize * (1 << (poolNumLevels - 1))
)

func initGetDataPools() []*sync.Pool {
	result := make([]*sync.Pool, poolNumLevels)
	for i := range result {
		level := i
		result[i] = &sync.Pool{
			New: func() any {
				return make([]byte, 0, 1<<(poolGetDataBaseSizeLog+level))
			},
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
	level := numBits - poolGetDataBaseSizeLog
	if numBits < poolGetDataBaseSizeLog {
		level = 0
	}

	result := getDataPools[level].Get().([]byte)
	return result[:size]
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

	data = data[:0]
	getDataPools[level].Put(data)
}
