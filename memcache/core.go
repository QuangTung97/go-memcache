package memcache

import (
	"github.com/QuangTung97/go-memcache/memcache/netconn"
	"sync"
	"sync/atomic"
)

type coreConnection struct {
	responseReader *responseReader
	sender         *sender

	shuttingDown uint32 // boolean
	wg           sync.WaitGroup

	// job data
	msgData []byte
	cmdList *cmdListReader
}

func newCoreConnection(nc netconn.NetConn, options *memcacheOptions) *coreConnection {
	cmdSender := newSender(nc, 10)

	c := &coreConnection{
		responseReader: newResponseReader(),
		sender:         cmdSender,

		shuttingDown: 0,

		msgData: make([]byte, options.bufferSize),
		cmdList: newCmdListReader(cmdSender),
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
}

func (c *coreConnection) waitReceiverShutdown() {
	c.wg.Wait()
}

func (c *coreConnection) publish(cmd *commandData) {
	c.sender.publish(cmd)
}

func (c *coreConnection) resetNetConn(nc netconn.NetConn) {
	c.sender.resetNetConn(nc)
}

func (c *coreConnection) waitForError() {
	c.sender.waitForError()
}

func (c *coreConnection) recvCommands() {
	defer c.wg.Done()

	for {
		err := c.recvSingleCommandData()
		if err == ErrConnClosed { // cmd len == 0
			return
		}
		if err != nil {
			c.responseReader.reset()

			reader := c.cmdList.current().reader
			c.sender.setNetConnError(err, reader)
		}
		c.cmdList.current().setCompleted(err)
		c.cmdList.next()
	}
}

func (c *coreConnection) recvSingleCommandData() error {
	cmdLen := c.cmdList.readIfExhausted()
	if cmdLen == 0 {
		return ErrConnClosed
	}

	current := c.cmdList.current()
	c.responseReader.setCurrentCommand(current)

	for count := 0; count < current.cmdCount; count++ {
		err := c.readNextMemcacheCommandResponse(current)
		if err != nil {
			return err
		}
	}

	c.responseReader.setCurrentCommand(nil)

	return nil
}

func (c *coreConnection) readNextMemcacheCommandResponse(current *commandData) error {
	for {
		// Read from response reader
		ok := c.responseReader.readNextData()
		if ok {
			return nil
		}

		if c.responseReader.hasError() != nil {
			return c.responseReader.hasError() // TODO testing
		}

		n, err := current.reader.Read(c.msgData)
		if err != nil {
			return err
		}

		c.responseReader.recv(c.msgData[:n])
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

func (c *cmdListReader) readIfExhausted() int {
	if c.offset >= c.length {
		c.length = c.sender.readSentCommands(c.cmdList)
		c.offset = 0
	}
	return c.length
}

func (c *cmdListReader) current() *commandData {
	return c.cmdList[c.offset]
}

func (c *cmdListReader) next() {
	c.offset++
}
