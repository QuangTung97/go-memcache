package memcache

import (
	"io"
	"sync"
)

// FlushWriter ...
type FlushWriter interface {
	io.Writer
	Flush() error
}

type closerInterface = io.Closer
type readCloserInterface = io.ReadCloser

//go:generate moq -out sender_mocks_test.go . FlushWriter closerInterface readCloserInterface

type noopFlusher struct {
	io.Writer
}

func (f noopFlusher) Flush() error {
	return nil
}

// NoopFlusher ...
func NoopFlusher(w io.Writer) FlushWriter {
	return noopFlusher{Writer: w}
}

type commandData struct {
	cmdCount  int
	data      []byte // for request and response data
	completed bool

	resetReader bool
	reader      io.ReadCloser

	lastErr error

	mut  sync.Mutex
	cond *sync.Cond
}

func newCommand() *commandData {
	c := &commandData{}
	c.cond = sync.NewCond(&c.mut)
	return c
}

func (c *commandData) waitCompleted() {
	c.mut.Lock()
	for !c.completed {
		c.cond.Wait()
	}
	c.mut.Unlock()
}

func (c *commandData) setCompleted(err error) {
	c.mut.Lock()
	c.completed = true
	c.lastErr = err
	c.mut.Unlock()

	c.cond.Signal()
}

type sender struct {
	nc          netConn
	ncMut       sync.Mutex
	tmpBuf      []*commandData
	lastErr     error
	ncErrorCond *sync.Cond

	sendBuf      []*commandData
	sendBufMax   int
	sendBufMut   sync.Mutex
	sendFullCond *sync.Cond

	recv *recvBuffer
}

type recvBuffer struct {
	buf []*commandData
	mut sync.Mutex

	mask  uint64
	begin uint64
	end   uint64

	sendCond *sync.Cond
	recvCond *sync.Cond
}

func newRecvBuffer(sizeLog int) *recvBuffer {
	b := &recvBuffer{
		buf:   make([]*commandData, 1<<sizeLog),
		mask:  1<<sizeLog - 1,
		begin: 0,
		end:   0,
	}
	b.sendCond = sync.NewCond(&b.mut)
	b.recvCond = sync.NewCond(&b.mut)
	return b
}

func (b *recvBuffer) push(cmdList []*commandData) {
	max := uint64(len(b.buf))

	for len(cmdList) > 0 {
		b.mut.Lock()

		// Wait until: begin + max - end > 0
		for b.begin+max <= b.end {
			b.sendCond.Wait()
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
}

func (b *recvBuffer) read(cmdList []*commandData) int {
	b.mut.Lock()
	for b.end == b.begin {
		b.recvCond.Wait()
	}

	n := uint64(len(cmdList))
	if b.end-b.begin < n {
		n = b.end - b.begin
	}

	for i := uint64(0); i < n; i++ {
		index := (b.begin + i) & b.mask
		cmdList[i] = b.buf[index]
	}
	b.begin += n

	b.mut.Unlock()
	b.sendCond.Signal()

	return int(n)
}

type netConn struct {
	writer FlushWriter
	closer io.Closer
	reader io.ReadCloser
}

func newSender(nc netConn, bufSizeLog int) *sender {
	maxSize := 1 << bufSizeLog
	s := &sender{
		nc:      nc,
		lastErr: nil,

		sendBuf:    make([]*commandData, 0, maxSize),
		sendBufMax: maxSize,

		recv: newRecvBuffer(bufSizeLog),
	}
	s.ncErrorCond = sync.NewCond(&s.ncMut)
	s.sendFullCond = sync.NewCond(&s.sendBufMut)
	return s
}

func clearCmdList(cmdList []*commandData) []*commandData {
	for i := range cmdList {
		cmdList[i] = nil
	}
	return cmdList[:0]
}

func (s *sender) replyErrorToCmdInTmpBuf(err error) {
	for _, cmd := range s.tmpBuf {
		cmd.setCompleted(err)
	}
	s.tmpBuf = clearCmdList(s.tmpBuf)
}

func (s *sender) setLastErrorAndClose(err error) {
	s.lastErr = err
	_ = s.nc.closer.Close()
}

func (s *sender) writeAndFlush() error {
	if s.lastErr != nil {
		return s.lastErr
	}

	for _, cmd := range s.tmpBuf {
		cmd.reader = s.nc.reader
		_, err := s.nc.writer.Write(cmd.data)
		if err != nil {
			s.setLastErrorAndClose(err)
			return err
		}
	}

	err := s.nc.writer.Flush()
	if err != nil {
		s.setLastErrorAndClose(err)
		return err
	}

	return nil
}

func (s *sender) sendToWriter() {
	s.ncMut.Lock()

	s.sendBufMut.Lock()
	s.tmpBuf = append(s.tmpBuf, s.sendBuf...)
	s.sendBuf = clearCmdList(s.sendBuf)
	s.sendBufMut.Unlock()

	s.sendFullCond.Signal()

	err := s.writeAndFlush()
	if err != nil {
		s.replyErrorToCmdInTmpBuf(err)
		s.ncMut.Unlock()
		return
	}

	s.recv.push(s.tmpBuf)
	s.tmpBuf = clearCmdList(s.tmpBuf)

	s.ncMut.Unlock()
}

func (s *sender) publish(cmd *commandData) {
	var prevLen int

	s.sendBufMut.Lock()
	for len(s.sendBuf) >= s.sendBufMax {
		s.sendFullCond.Wait()
	}

	prevLen = len(s.sendBuf)
	s.sendBuf = append(s.sendBuf, cmd)
	s.sendBufMut.Unlock()

	if prevLen > 0 {
		return
	}

	s.sendToWriter()
}

func (s *sender) readSentCommands(cmdList []*commandData) int {
	return s.recv.read(cmdList)
}

func (s *sender) waitForError() {
	s.ncMut.Lock()
	for s.lastErr == nil {
		s.ncErrorCond.Wait()
	}
	s.ncMut.Unlock()
}

func (s *sender) resetNetConn(nc netConn, err error) {
	s.ncMut.Lock()
	s.nc = nc
	s.lastErr = err
	s.ncMut.Unlock()
}

func (s *sender) closeNetConn() error {
	s.ncMut.Lock()
	err := s.nc.closer.Close()
	s.lastErr = ErrConnClosed
	s.ncMut.Unlock()
	return err
}
