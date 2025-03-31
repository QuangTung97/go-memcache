package memcache

import (
	"errors"
	"sync"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

type coreConnection struct {
	responseReader *responseReader
	sender         *sender

	wg sync.WaitGroup

	// job data
	msgData []byte
	cmdList *cmdListReader
}

func newCoreConnection(nc netconn.NetConn, options *memcacheOptions) *coreConnection {
	cmdSender := newSender(nc, 7, options.writeLimit)

	c := &coreConnection{
		responseReader: newResponseReader(),
		sender:         cmdSender,

		msgData: make([]byte, options.bufferSize),
		cmdList: newCmdListReader(cmdSender),
	}

	c.wg.Add(1)
	go c.recvCommands()

	return c
}

func (c *coreConnection) waitReceiverShutdown() {
	c.wg.Wait()
}

func (c *coreConnection) publish(cmd *commandListData) {
	c.sender.publish(cmd)
}

func (c *coreConnection) resetNetConn(nc netconn.NetConn) {
	c.sender.resetNetConn(nc)
}

func (c *coreConnection) waitForError() (closed bool) {
	return c.sender.waitForError()
}

func (c *coreConnection) recvCommands() {
	defer c.wg.Done()

	for {
		var inc increaseReadCount
		err := c.recvSingleCommandData(&inc)
		if err != nil {
			if errors.Is(err, ErrConnClosed) { // cmd len == 0
				c.sender.shutdown()
				return
			}

			c.responseReader.reset()
		}
		c.cmdList.current().setCompleted(err)
		c.cmdList.next()
	}
}

func (c *coreConnection) recvSingleCommandData(inc *increaseReadCount) error {
	cmdLen := c.cmdList.readIfExhausted()
	if cmdLen == 0 {
		return ErrConnClosed
	}

	current := c.cmdList.current()
	c.responseReader.setCurrentCommand(current)

	for count := 0; count < current.cmdCount; count++ {
		err := c.readNextMemcacheCommandResponse(current, inc)
		if err != nil {
			return err
		}
	}

	c.responseReader.setCurrentCommand(nil)

	return nil
}

type increaseReadCount struct {
	increased bool
}

func (i *increaseReadCount) apply(c *coreConnection, current *commandListData) {
	if i.increased {
		return
	}
	i.increased = true
	c.sender.selector.addReadCount(uint64(current.cmdCount))
}

func (c *coreConnection) readNextMemcacheCommandResponse(current *commandListData, inc *increaseReadCount) error {
	for {
		// Read from response reader
		ok := c.responseReader.readNextData()
		if ok {
			inc.apply(c, current)

			if c.responseReader.hasError() != nil {
				err := c.responseReader.hasError()
				return current.conn.setLastErrorAndClose(err)
			}
			return nil
		}

		n, err := current.conn.readData(c.msgData)

		inc.apply(c, current)

		if err != nil {
			return err
		}

		c.responseReader.recv(c.msgData[:n])
	}
}

type cmdListReader struct {
	sender *sender

	cmdList []*commandListData
	length  int
	offset  int
}

func newCmdListReader(sender *sender) *cmdListReader {
	return &cmdListReader{
		sender:  sender,
		cmdList: make([]*commandListData, 128),
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

func (c *cmdListReader) current() *commandListData {
	return c.cmdList[c.offset]
}

func (c *cmdListReader) next() {
	c.cmdList[c.offset] = nil
	c.offset++
}
