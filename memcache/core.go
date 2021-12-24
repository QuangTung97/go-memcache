package memcache

import (
	"io"
	"sync"
	"sync/atomic"
	"unsafe"
)

type netConn struct {
	reader  io.ReadCloser
	writer  FlushWriter
	lastErr error
}

type coreConnection struct {
	nc unsafe.Pointer // point to netConn

	responseReader *responseReader
	sender         *sender

	shuttingDown uint32 // boolean
	wg           sync.WaitGroup
}

func newCoreConnection(reader io.ReadCloser, writer FlushWriter) *coreConnection {
	c := &coreConnection{
		nc: unsafe.Pointer(&netConn{
			reader:  reader,
			writer:  writer,
			lastErr: nil,
		}),

		responseReader: newResponseReader(21),
		sender:         newSender(writer, 10),

		shuttingDown: 0,
	}
	c.wg.Add(1)
	go c.recvCommands()
	return c
}

func (c *coreConnection) isShuttingDown() bool {
	return atomic.LoadUint32(&c.shuttingDown) > 0
}

func (c *coreConnection) shutdown() {
	atomic.StoreUint32(&c.shuttingDown, 1)
	c.wg.Wait()
}

func (c *coreConnection) publish(cmd *commandData) error {
	return c.sender.publish(cmd)
}

func (c *coreConnection) recvCommands() {
	defer c.wg.Done()

	for !c.isShuttingDown() {
		nc := (*netConn)(atomic.LoadPointer(&c.nc))
		err := c.recvCommandsForReader(nc.reader)
		nc.lastErr = err
	}
}

func (c *coreConnection) recvCommandsForReader(reader io.ReadCloser) error {
	tmpData := make([]byte, 1<<21)
	msg := make([]byte, 2048)
	cmdList := newCmdListReader(c.sender)

	responseCount := 0

	for {
		cmdList.readIfExhausted()

		n, err := reader.Read(msg)
		if err != nil {
			return err
		}

		c.responseReader.recv(msg[:n])

		for {
			size, ok := c.responseReader.getNext()
			if !ok {
				break
			}

			c.responseReader.readData(tmpData[:size])

			current := cmdList.current()

			if responseCount == 0 {
				current.data = current.data[:0]
			}
			current.data = append(current.data, tmpData[:size]...) // need optimize??
			responseCount++

			if responseCount < current.cmdCount {
				continue
			}

			cmdList.next()

			responseCount = 0

			current.mut.Lock()
			current.completed = true
			current.mut.Unlock()
			current.cond.Signal()

			break
		}
	}
}

type cmdListReader struct {
	sender *sender

	cmdList []*commandData
	length  int
	offset  int
}

func newCmdListReader(sender *sender) *cmdListReader {
	return &cmdListReader{
		sender:  sender,
		cmdList: make([]*commandData, 128),
		length:  0,
		offset:  0,
	}
}

func (c *cmdListReader) readIfExhausted() {
	if c.offset >= c.length {
		c.length = c.sender.readSentCommands(c.cmdList)
		c.offset = 0
	}
}

func (c *cmdListReader) current() *commandData {
	return c.cmdList[c.offset]
}

func (c *cmdListReader) next() {
	c.offset++
}
