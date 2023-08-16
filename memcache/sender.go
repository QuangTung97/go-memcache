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

type commandData struct {
	cmdCount int

	requestData  []byte
	responseData []byte

	responseBinaries [][]byte // mget binary responses

	reader io.ReadCloser

	lastErrAtomic atomic.Pointer[lastError]
	lastErr       error

	ch chan error
}

type lastError struct {
	err error
}

func newCommandChannel() chan error {
	return make(chan error, 1)
}

func newCommand() *commandData {
	c := &commandData{
		requestData:  requestBytesPool.get(),
		responseData: responseBytesPool.get(),
	}
	c.ch = newCommandChannel()
	return c
}

func freeCommandResponseData(cmd *commandData) {
	responseBytesPool.put(cmd.responseData)
	cmd.responseData = nil
	for i := range cmd.responseBinaries {
		cmd.responseBinaries[i] = nil
	}
	cmd.responseBinaries = nil
}

func freeCommandRequestData(cmd *commandData) {
	requestBytesPool.put(cmd.requestData)
	cmd.requestData = nil
}

func (c *commandData) waitCompleted() {
	err := <-c.ch

	atomicErr := c.lastErrAtomic.Load()
	if atomicErr != nil && atomicErr.err != nil {
		err = atomicErr.err
	}

	c.lastErr = err
}

func (c *commandData) setErrorOnly(err error) {
	c.lastErrAtomic.Store(&lastError{
		err: err,
	})
}

func (c *commandData) setCompleted(err error) {
	c.ch <- err
}

type sender struct {
	// ---- protected by ncMut ------
	nc netconn.NetConn

	alreadyClosed bool

	ncMut  sync.Mutex
	tmpBuf []*commandData

	lastErr     error
	ncErrorCond *sync.Cond
	// ---- end ncMut protection ----

	send sendBuffer
	recv recvBuffer
}

// ----------------------------------
// Send Buffer
// ----------------------------------
type sendBuffer struct {
	buf    []*commandData
	maxLen int
	mut    sync.Mutex
	cond   *sync.Cond
}

func initSendBuffer(b *sendBuffer, bufSizeLog int) {
	maxSize := 1 << bufSizeLog
	b.buf = make([]*commandData, 0, maxSize)
	b.maxLen = maxSize

	b.cond = sync.NewCond(&b.mut)
}

func (b *sendBuffer) popAll(output []*commandData) []*commandData {
	b.mut.Lock()
	output = append(output, b.buf...)

	// clear buffer
	for i := range b.buf {
		b.buf[i] = nil
	}
	b.buf = b.buf[:0]

	b.mut.Unlock()

	b.cond.Broadcast()
	return output
}

func (b *sendBuffer) push(cmd *commandData) (isLeader bool) {
	b.mut.Lock()
	for len(b.buf) >= b.maxLen {
		b.cond.Wait()
	}

	prevLen := len(b.buf)
	b.buf = append(b.buf, cmd)
	b.mut.Unlock()

	return prevLen == 0
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

func newSender(nc netconn.NetConn, bufSizeLog int) *sender {
	s := &sender{
		nc:      nc,
		lastErr: nil,
	}

	initSendBuffer(&s.send, bufSizeLog)
	initRecvBuffer(&s.recv, bufSizeLog+1)

	s.ncErrorCond = sync.NewCond(&s.ncMut)
	return s
}

func clearCmdList(cmdList []*commandData) []*commandData {
	for i, cmd := range cmdList {
		freeCommandRequestData(cmd)
		cmdList[i] = nil
	}
	return cmdList[:0]
}

func (s *sender) setLastErrorAndClose(err error) error {
	s.lastErr = err
	s.ncErrorCond.Signal()
	if !s.alreadyClosed {
		s.alreadyClosed = true
		return s.nc.Closer.Close()
	}
	return nil
}

func (s *sender) writeAndFlush() error {
	if s.lastErr != nil {
		return s.lastErr
	}

	for _, cmd := range s.tmpBuf {
		_, err := s.nc.Writer.Write(cmd.requestData)
		if err != nil {
			_ = s.setLastErrorAndClose(err)
			return err
		}
	}

	err := s.nc.Writer.Flush()
	if err != nil {
		_ = s.setLastErrorAndClose(err)
		return err
	}

	return nil
}

func (s *sender) sendToWriter() {
	s.ncMut.Lock()

	s.tmpBuf = s.send.popAll(s.tmpBuf)

	for _, cmd := range s.tmpBuf {
		cmd.reader = s.nc.Reader
	}

	remainingCommands, closed := s.recv.push(s.tmpBuf)
	if closed {
		for _, cmd := range remainingCommands {
			cmd.setCompleted(ErrConnClosed)
		}
		s.tmpBuf = clearCmdList(s.tmpBuf)
		s.ncMut.Unlock()
		return
	}

	// flush to tcp socket
	err := s.writeAndFlush()
	if err != nil {
		for _, cmd := range s.tmpBuf {
			cmd.setErrorOnly(err)
		}
		s.tmpBuf = clearCmdList(s.tmpBuf)
		s.ncMut.Unlock()
		return
	}

	s.tmpBuf = clearCmdList(s.tmpBuf)
	s.ncMut.Unlock()
}

func (s *sender) publish(cmd *commandData) {
	isLeader := s.send.push(cmd)
	if !isLeader {
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

// setNetConnError used inside *sender.go*
func (s *sender) setNetConnError(err error, prevReader io.ReadCloser) {
	s.ncMut.Lock()
	if s.lastErr != ErrConnClosed {
		if s.nc.Reader == prevReader {
			_ = s.setLastErrorAndClose(err)
		}
	}
	s.ncMut.Unlock()
}

func (s *sender) resetNetConn(nc netconn.NetConn) {
	s.ncMut.Lock()
	if s.lastErr != ErrConnClosed {
		s.nc = nc
		s.alreadyClosed = false
		s.lastErr = nil
	}
	s.ncMut.Unlock()
}

func (s *sender) closeNetConn() error {
	s.ncMut.Lock()

	s.recv.closeBuffer()
	err := s.setLastErrorAndClose(ErrConnClosed)

	s.ncMut.Unlock()

	return err
}
