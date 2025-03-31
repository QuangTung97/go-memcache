package memcache

import (
	"io"
	"sync"
)

type commandType int

const (
	commandTypeMGet commandType = iota + 1
	commandTypeMSet
	commandTypeMDel
	commandTypeFlushAll
	commandTypeVersion
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

// commandListData is the main data-structure for storing:
// - list of encoded commands produced by **cmdBuilder**.
// - **cmdCount** is the number of commands (max value = memcacheOptions.maxCommandsPerBatch).
type commandListData struct {
	cmdCount int

	sibling *commandListData // for commands of the same pipeline
	link    *commandListData // for linking to form a list

	requestData  []byte
	responseData []byte

	requestBinaries  *requestBinaryEntry // linked list of binary entries
	responseBinaries [][]byte            // mget binary responses

	conn *senderConnection

	lastErr error
	ch      chan error
}

type requestBinaryEntry struct {
	next   *requestBinaryEntry // to form a linked list of binary entries
	offset int
	data   []byte
}

func newCommandChannel() chan error {
	return make(chan error, 1)
}

func newCommand() *commandListData {
	c := &commandListData{
		requestData:  requestBytesPool.get(),
		responseData: responseBytesPool.get(),
	}
	c.ch = newCommandChannel()
	return c
}

func (c *commandListData) writeToWriter(w io.Writer) error {
	index := 0
	for current := c.requestBinaries; current != nil; current = current.next {
		if _, err := w.Write(c.requestData[index:current.offset]); err != nil {
			return err
		}
		if _, err := w.Write(current.data); err != nil {
			return err
		}
		index = current.offset
	}

	if index < len(c.requestData) {
		if _, err := w.Write(c.requestData[index:]); err != nil {
			return err
		}
	}

	return nil
}

func freeCommandResponseData(cmd *commandListData) {
	responseBytesPool.put(cmd.responseData)
	cmd.responseData = nil

	for i := range cmd.responseBinaries {
		cmd.responseBinaries[i] = nil
	}
	putResponseBinaries(cmd.responseBinaries)
	cmd.responseBinaries = nil
}

func freeCommandRequestData(cmd *commandListData) {
	requestBytesPool.put(cmd.requestData)
	cmd.requestData = nil

	for current := cmd.requestBinaries; current != nil; current = current.next {
		releaseByteSlice(current.data)
	}
	cmd.requestBinaries = nil
}

func (c *commandListData) waitCompleted() {
	err := <-c.ch
	c.lastErr = err
}

func (c *commandListData) setCompleted(err error) {
	c.ch <- err
}
