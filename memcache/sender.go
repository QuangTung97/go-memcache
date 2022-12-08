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

//=====================
// Pool of Bytes
//=====================
var commandDataBytesPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 256)
	},
}

func getBytesFromPool() []byte {
	data := commandDataBytesPool.Get().([]byte)
	return data
}

func putBytesToPool(data []byte) {
	data = data[:0]
	commandDataBytesPool.Put(data)
}

type commandData struct {
	cmdCount int

	requestData  []byte
	responseData []byte

	completed bool

	epoch  uint64
	reader io.ReadCloser

	lastErr error

	mut  sync.Mutex
	cond *sync.Cond
}

func newCommand() *commandData {
	c := &commandData{
		requestData: getBytesFromPool(),
	}
	c.cond = sync.NewCond(&c.mut)
	return c
}

func (c *commandData) cloneShareRequest() *commandData {
	result := &commandData{
		cmdCount:    c.cmdCount,
		requestData: c.requestData,
	}
	result.cond = sync.NewCond(&result.mut)
	return result
}

func freeCommandData(cmd *commandData) {
	putBytesToPool(cmd.requestData)
	putBytesToPool(cmd.responseData)
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
	if c.completed {
		c.mut.Unlock()
		return
	}

	c.completed = true
	c.lastErr = err
	c.mut.Unlock()

	c.cond.Signal()
}

type sender struct {
	//---- protected by ncMut ------
	nc     netConn
	ncMut  sync.Mutex
	tmpBuf []*commandData

	epoch       uint64
	lastErr     error
	ncErrorCond *sync.Cond
	//---- end ncMut protection ----

	send sendBuffer
	recv recvBuffer
}

//----------------------------------
// Send Buffer
//----------------------------------
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
	b.buf = clearCmdList(b.buf)
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

//----------------------------------
// Receiving Buffer
//----------------------------------
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

func (b *recvBuffer) push(cmdList []*commandData) {
	max := uint64(len(b.buf))

	for len(cmdList) > 0 {
		b.mut.Lock()

		if b.closed {
			b.mut.Unlock()
			return
		}

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

	b.recvCond.Signal()
}

type netConn struct {
	writer FlushWriter
	closer io.Closer
	reader io.ReadCloser
}

func newSender(nc netConn, bufSizeLog int) *sender {
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
	s.ncErrorCond.Signal()
	_ = s.nc.closer.Close()
}

func (s *sender) writeAndFlush() error {
	if s.lastErr != nil {
		return s.lastErr
	}

	for _, cmd := range s.tmpBuf {
		_, err := s.nc.writer.Write(cmd.requestData)
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

	s.tmpBuf = s.send.popAll(s.tmpBuf)

	for _, cmd := range s.tmpBuf {
		cmd.reader = s.nc.reader
		if s.lastErr == nil {
			cmd.epoch = s.epoch
		}
	}

	s.recv.push(s.tmpBuf)

	// flush to tcp socket
	err := s.writeAndFlush()
	if err != nil {
		s.replyErrorToCmdInTmpBuf(err)
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
		if s.nc.reader == prevReader {
			s.lastErr = err
		}
	}
	s.ncMut.Unlock()

	s.ncErrorCond.Signal()
}

// waitForNewEpoch used inside *pipeline.go*
func (s *sender) waitForNewEpoch(waitEpoch uint64) error {
	s.ncMut.Lock()
	for s.lastErr != ErrConnClosed && s.epoch <= waitEpoch {
		s.ncErrorCond.Wait()
	}
	err := s.lastErr
	s.ncMut.Unlock()
	return err
}

// increaseEpochAndSetError used inside *conn.go*
func (s *sender) increaseEpochAndSetError(err error) {
	s.ncMut.Lock()
	s.epoch++
	s.lastErr = err
	s.ncMut.Unlock()

	s.ncErrorCond.Broadcast()
}

func (s *sender) resetNetConn(nc netConn) {
	s.ncMut.Lock()
	s.epoch++
	s.nc = nc
	s.lastErr = nil
	s.ncMut.Unlock()

	s.ncErrorCond.Broadcast()
}

func (s *sender) closeNetConn() error {
	s.ncMut.Lock()

	s.recv.closeBuffer()
	err := s.nc.closer.Close()
	s.lastErr = ErrConnClosed

	s.ncMut.Unlock()

	s.ncErrorCond.Broadcast()

	return err
}
