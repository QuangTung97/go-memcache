package memcache

import (
	"bufio"
	"net"
)

type conn struct {
	nc net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	responseReader *responseReader
	sender         *sender
}

func newConn(addr string) (*conn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(nc)

	c := &conn{
		nc:     nc,
		reader: bufio.NewReader(nc),
		writer: writer,

		responseReader: newResponseReader(21),
		sender:         newSender(writer, 10),
	}

	go c.recvCommands()

	return c, nil
}

func (c *conn) pushCommand(cmd *commandData) {
	c.sender.publish(cmd)
}

func (c *conn) recvCommands() {
	tmpData := make([]byte, 1<<21)
	msg := make([]byte, 2048)
	cmdList := newCmdListReader(c.sender)

	responseCount := 0

	for {
		cmdList.readIfExhausted()

		n, err := c.reader.Read(msg)
		if err != nil {
			panic(err) // TODO
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
