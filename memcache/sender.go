package memcache

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

type closerInterface = io.Closer
type readCloserInterface = io.ReadCloser

// FlushWriter ...
type FlushWriter = netconn.FlushWriter

//go:generate moq -out sender_mocks_test.go . FlushWriter closerInterface readCloserInterface

type sender struct {
	// ---- protected by connMut ------
	connMut     sync.Mutex
	conn        *senderConnection
	tmpBuf      []*commandData
	ncErrorCond *sync.Cond
	closed      bool
	// ---- end connMut protection ----

	finishCh chan struct{}
	sendBuf  sendBuffer
	selector inputSelector
	recv     recvBuffer
}

type senderConnection struct {
	sender *sender

	writer FlushWriter
	reader io.Reader
	closer io.Closer

	lastErr atomic.Pointer[senderLastError]
}

func newSenderConn(s *sender, nc netconn.NetConn) *senderConnection {
	return &senderConnection{
		sender: s,

		writer: nc.Writer,
		reader: nc.Reader,
		closer: nc.Closer,
	}
}

type senderLastError struct {
	err error
}

func (c *senderConnection) setLastErrorInternal(err error) {
	c.lastErr.Store(&senderLastError{
		err: err,
	})
}

func (c *senderConnection) getLastErrorInternal() error {
	v := c.lastErr.Load()
	if v == nil {
		return nil
	}
	return v.err
}

func (c *senderConnection) setLastErrorAndCloseUnsafe(err error) error {
	lastErr := c.getLastErrorInternal()
	if lastErr != nil {
		return lastErr
	}
	c.setLastErrorInternal(err)
	_ = c.closer.Close()
	c.sender.ncErrorCond.Signal()
	return err
}

func (c *senderConnection) setLastErrorAndClose(err error) error {
	c.sender.connMut.Lock()
	resultErr := c.setLastErrorAndCloseUnsafe(err)
	c.sender.connMut.Unlock()
	return resultErr
}

func (c *senderConnection) readData(data []byte) (int, error) {
	lastErr := c.getLastErrorInternal()
	if lastErr != nil {
		return 0, lastErr
	}

	n, err := c.reader.Read(data)
	if err != nil {
		return n, c.setLastErrorAndClose(err)
	}

	return n, nil
}

// ----------------------------------
// Receiving Buffer
// ----------------------------------
type recvBuffer struct {
	buf []*commandData
	mut sync.Mutex

	mask  uint64
	begin uint64
	end   uint64

	closed bool

	sendCond *sync.Cond
	recvCond *sync.Cond
}

func initRecvBuffer(b *recvBuffer, sizeLog int) {
	b.buf = make([]*commandData, 1<<sizeLog)
	b.mask = 1<<sizeLog - 1
	b.begin = 0
	b.end = 0
	b.sendCond = sync.NewCond(&b.mut)
	b.recvCond = sync.NewCond(&b.mut)
}

func (b *recvBuffer) push(cmdList []*commandData) (remaining []*commandData, isClosed bool) {
	max := uint64(len(b.buf))

	for len(cmdList) > 0 {
		b.mut.Lock()

		// Wait until: begin + max - end > 0
		for !b.closed && b.begin+max <= b.end {
			b.sendCond.Wait()
		}

		if b.closed {
			b.mut.Unlock()
			return cmdList, true
		}

		n := uint64(len(cmdList))
		if b.begin+max < b.end+n {
			n = b.begin + max - b.end
		}

		for i, cmd := range cmdList[:n] {
			index := (b.end + uint64(i)) & b.mask
			b.buf[index] = cmd
		}
		b.end += n

		b.mut.Unlock()
		b.recvCond.Signal()

		cmdList = cmdList[n:]
	}

	return nil, false
}

func (b *recvBuffer) read(cmdList []*commandData) int {
	b.mut.Lock()
	for !b.closed && b.end == b.begin {
		b.recvCond.Wait()
	}

	n := uint64(len(cmdList))
	if b.end-b.begin < n {
		n = b.end - b.begin
	}

	for i := uint64(0); i < n; i++ {
		index := (b.begin + i) & b.mask
		cmdList[i] = b.buf[index]
		b.buf[index] = nil
	}
	b.begin += n

	b.mut.Unlock()
	b.sendCond.Signal()

	return int(n)
}

func (b *recvBuffer) closeBuffer() {
	b.mut.Lock()
	b.closed = true
	b.mut.Unlock()

	b.sendCond.Signal()
	b.recvCond.Signal()
}

func newSender(nc netconn.NetConn, bufSizeLog int, writeLimit int) *sender {
	s := &sender{}
	s.conn = newSenderConn(s, nc)
	s.ncErrorCond = sync.NewCond(&s.connMut)

	initSendBuffer(&s.sendBuf)
	initInputSelector(&s.selector, &s.sendBuf, writeLimit, 1<<bufSizeLog)

	initRecvBuffer(&s.recv, bufSizeLog+1)

	s.finishCh = make(chan struct{})
	go s.runSenderJob()

	return s
}

func clearCmdList(cmdList []*commandData) []*commandData {
	for i, cmd := range cmdList {
		freeCommandRequestData(cmd)
		cmdList[i] = nil
	}
	return cmdList[:0]
}

func (s *sender) writeAndFlush() {
	if s.conn.getLastErrorInternal() != nil {
		return
	}

	for _, cmd := range s.tmpBuf {
		_, err := s.conn.writer.Write(cmd.requestData)
		if err != nil {
			_ = s.conn.setLastErrorAndCloseUnsafe(err)
			return
		}
	}

	err := s.conn.writer.Flush()
	if err != nil {
		_ = s.conn.setLastErrorAndCloseUnsafe(err)
		return
	}
}

func (s *sender) sendToWriter() (closed bool) {
	s.tmpBuf, closed = s.selector.readCommands(s.tmpBuf)

	s.connMut.Lock()

	for _, cmd := range s.tmpBuf {
		cmd.conn = s.conn
	}

	s.recv.push(s.tmpBuf)

	// flush to tcp socket
	s.writeAndFlush()

	s.tmpBuf = clearCmdList(s.tmpBuf)
	s.connMut.Unlock()

	return closed
}

func (s *sender) publish(cmd *commandData) {
	closed := s.sendBuf.push(cmd)
	if closed {
		cmd.setCompleted(ErrConnClosed)
	}
}

func (s *sender) runSenderJob() {
	defer func() {
		s.recv.closeBuffer()
		close(s.finishCh)
	}()

	for {
		closed := s.sendToWriter()
		if closed {
			return
		}
	}
}

func (s *sender) readSentCommands(cmdList []*commandData) int {
	return s.recv.read(cmdList)
}

func (s *sender) waitForError() (closed bool) {
	s.connMut.Lock()
	result := s.closed
	for !s.closed && s.conn.getLastErrorInternal() == nil {
		s.ncErrorCond.Wait()
	}
	s.connMut.Unlock()
	return result
}

func (s *sender) resetNetConn(nc netconn.NetConn) {
	s.connMut.Lock()
	if s.closed {
		_ = nc.Closer.Close()
	} else {
		s.conn = newSenderConn(s, nc)
	}
	s.connMut.Unlock()
}

func (s *sender) closeSendJob() {
	s.sendBuf.close()
}

func (s *sender) waitForClosingSendJob() {
	<-s.finishCh
}

func (s *sender) shutdown() {
	s.connMut.Lock()
	s.closed = true
	_ = s.conn.setLastErrorAndCloseUnsafe(ErrConnClosed)
	s.connMut.Unlock()
}
