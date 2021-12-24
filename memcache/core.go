package memcache

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type coreConnection struct {
	nc unsafe.Pointer // point to netConn

	responseReader *responseReader
	sender         *sender

	shuttingDown uint32 // boolean
	wg           sync.WaitGroup
}

func newCoreConnection(nc netConn) *coreConnection {
	c := &coreConnection{
		responseReader: newResponseReader(21),
		sender:         newSender(nc, 10),

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
		_ = c.recvCommandsSingleLoop()
	}
}

//revive:disable:cognitive-complexity
func (c *coreConnection) recvCommandsSingleLoop() error {
	tmpData := make([]byte, 1<<21)
	msg := make([]byte, 2048)
	cmdList := newCmdListReader(c.sender)

	responseCount := 0

	for {
		cmdList.readIfExhausted()

		current := cmdList.current()

		n, err := current.reader.Read(msg)
		if err != nil {
			return err
		}

		if current.resetReader {
			current.resetReader = false
			c.responseReader.reset()
		}
		c.responseReader.recv(msg[:n])

		for {
			size, ok := c.responseReader.getNext()
			if !ok {
				break
			}

			c.responseReader.readData(tmpData[:size])

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

//revive:enable:cognitive-complexity

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
