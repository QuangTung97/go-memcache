package memcache

import (
	"bufio"
	"net"
	"sync"
)

type sendBuffer struct {
	mut sync.Mutex
	buf []*commandData
}

func newSendBuffer(capacity int) *sendBuffer {
	return &sendBuffer{
		buf: make([]*commandData, 0, capacity),
	}
}

func (b *sendBuffer) push(cmd *commandData) {
	b.buf = append(b.buf, cmd)
}

func (b *sendBuffer) clearRemain(n int) {
	remain := len(b.buf) - n
	for i := n; i < len(b.buf); i++ {
		b.buf[i-n] = b.buf[i]
	}
	for i := remain; i < len(b.buf); i++ {
		b.buf[i] = nil
	}
	b.buf = b.buf[:remain]
}

type connBuffer struct {
	mut      sync.Mutex
	sendCond *sync.Cond
	recvCond *sync.Cond

	begin int
	end   int
	buf   []*commandData
}

func newConnBuffer(size int) *connBuffer {
	buf := &connBuffer{
		begin: 0,
		end:   0,
		buf:   make([]*commandData, size),
	}

	buf.sendCond = sync.NewCond(&buf.mut)
	buf.recvCond = sync.NewCond(&buf.mut)

	return buf
}

type conn struct {
	nc net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	sendBuf *sendBuffer
	connBuf *connBuffer
}

func newConn(addr string) (*conn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &conn{
		nc:     nc,
		reader: bufio.NewReader(nc),
		writer: bufio.NewWriter(nc),

		sendBuf: newSendBuffer(128),
		connBuf: newConnBuffer(128),
	}, nil
}

func (c *conn) waitForFreeSpaces() int {
	for {
		freeSpaces := len(c.connBuf.buf) + c.connBuf.begin - c.connBuf.end
		if freeSpaces > 0 {
			return freeSpaces
		}
		c.connBuf.sendCond.Wait()
	}
}

func (c *conn) publish(n int) {
	c.connBuf.end = (c.connBuf.end + n) % len(c.connBuf.buf)
}

func (c *conn) copyFromSendBuf(freeSpaces int) int {
	c.sendBuf.mut.Lock()
	n := len(c.sendBuf.buf)
	if n > freeSpaces {
		n = freeSpaces
	}

	firstPart := len(c.connBuf.buf) - c.connBuf.end
	copy(c.connBuf.buf[c.connBuf.end:], c.sendBuf.buf[:n])
	if firstPart < n {
		copy(c.connBuf.buf, c.sendBuf.buf[firstPart:])
	}

	c.sendBuf.clearRemain(n)
	c.sendBuf.mut.Unlock()

	return n
}

func (c *conn) sendCommands(prevEnd int, newEnd int) {
	for i := prevEnd; i != newEnd; i = (i + 1) % len(c.connBuf.buf) {
		cmd := c.connBuf.buf[i]
		cmd.mut.Lock()
		_, err := c.writer.Write(cmd.data)
		if err != nil {
			panic(err) // TODO
		}
		cmd.mut.Unlock()
	}
}

func (c *conn) pushCommand(cmd *commandData) {
	var prevLen int

	if prevLen > 0 {
		return
	}

	c.connBuf.mut.Lock()
	freeSpaces := c.waitForFreeSpaces()
	n := c.copyFromSendBuf(freeSpaces)

	prevEnd := c.connBuf.end
	c.connBuf.end = (c.connBuf.end + n) % len(c.connBuf.buf)
	newEnd := c.connBuf.end
	c.connBuf.mut.Unlock()
	c.connBuf.recvCond.Signal()

	c.sendCommands(prevEnd, newEnd)
}

func (c *conn) waitForActiveCommands() (int, int) {
	for {
		begin := c.connBuf.begin
		end := c.connBuf.end

		if begin < end {
			return begin, end
		}
		c.connBuf.recvCond.Wait()
	}
}

func (c *conn) recvCommands() {
	//data := make([]byte, 2048)
	//
	//c.connBuf.mut.Lock()
	//begin, end := c.waitForActiveCommands()
	//c.connBuf.mut.Unlock()
	//
	//for {
	//	n, err := c.reader.Read(data)
	//	if err != nil {
	//		panic(err)
	//	}
	//
	//	for i := 0; i < n; i++ {
	//	}
	//}
}
