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

	cmdList := make([]*commandData, 128)
	cmdListLen := 0
	cmdListOffset := 0

	msg := make([]byte, 2048)

	responseCount := 0

	for {
		if cmdListOffset >= cmdListLen {
			cmdListLen = c.sender.readSentCommands(cmdList)
			cmdListOffset = 0
		}

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

			current := cmdList[cmdListOffset]
			if responseCount == 0 {
				current.data = current.data[:0]
			}
			current.data = append(current.data, tmpData[:size]...) // need optimize??
			responseCount++

			if responseCount < current.cmdCount {
				continue
			}

			cmdListOffset++
			responseCount = 0

			current.mut.Lock()
			current.completed = true
			current.mut.Unlock()
			current.cond.Signal()

			break
		}
	}
}
