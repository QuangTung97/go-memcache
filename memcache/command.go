package memcache

import (
	"sync"
)

type commandType int

const (
	commandTypeMGet commandType = iota + 1
	commandTypeMSet
	commandTypeMDel
	commandTypeFlushAll
)

// =====================
// Pool of Bytes
// =====================
var requestBytesPool = bytesPool{
	pool: sync.Pool{
		New: func() any {
			return make([]byte, 0, 256)
		},
	},
}

var responseBytesPool = bytesPool{
	pool: sync.Pool{
		New: func() any {
			return make([]byte, 0, 1024)
		},
	},
}

type bytesPool struct {
	pool sync.Pool
}

func (p *bytesPool) get() []byte {
	return p.pool.Get().([]byte)
}

func (p *bytesPool) put(data []byte) {
	data = data[:0]
	p.pool.Put(data)
}

type commandData struct {
	cmdCount int

	requestData  []byte
	responseData []byte

	responseBinaries [][]byte // mget binary responses

	conn *senderConnection

	lastErr error
	ch      chan error
}

func newCommandChannel() chan error {
	return make(chan error, 1)
}

func newCommand() *commandData {
	c := &commandData{
		requestData:  requestBytesPool.get(),
		responseData: responseBytesPool.get(),
	}
	c.ch = newCommandChannel()
	return c
}

func freeCommandResponseData(cmd *commandData) {
	responseBytesPool.put(cmd.responseData)
	cmd.responseData = nil
	for i := range cmd.responseBinaries {
		cmd.responseBinaries[i] = nil
	}
	cmd.responseBinaries = nil
}

func freeCommandRequestData(cmd *commandData) {
	requestBytesPool.put(cmd.requestData)
	cmd.requestData = nil
}

func (c *commandData) waitCompleted() {
	err := <-c.ch
	c.lastErr = err
}

func (c *commandData) setCompleted(err error) {
	c.ch <- err
}
