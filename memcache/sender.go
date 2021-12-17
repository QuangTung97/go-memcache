package memcache

import (
	"io"
	"sync"
)

type commandData struct {
	cmdCount  int
	data      []byte // for request and response data
	completed bool

	mut  sync.Mutex
	cond *sync.Cond
}

func newCommand() *commandData {
	c := &commandData{}
	c.cond = sync.NewCond(&c.mut)
	return c
}

type sender struct {
	writer    io.Writer
	writerMut sync.Mutex
	tmpBuf    []*commandData

	sendBuf    []*commandData
	sendBufMut sync.Mutex

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
	b.mut.Lock()
	n := uint64(len(cmdList))
	max := uint64(len(b.buf))

	// Wait until: begin + max - end >= len(cmdList)
	for b.begin+max < b.end+n {
		b.sendCond.Wait()
	}

	for i, cmd := range cmdList {
		index := (b.end + uint64(i)) & b.mask
		b.buf[index] = cmd
	}
	b.end += n

	b.mut.Unlock()

	b.recvCond.Signal()
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

func newSender(writer io.Writer) *sender {
	return &sender{
		writer:  writer,
		sendBuf: make([]*commandData, 0, 128),
		recv:    newRecvBuffer(7), // 128
	}
}

func clearCmdList(cmdList []*commandData) []*commandData {
	for i := range cmdList {
		cmdList[i] = nil
	}
	return cmdList[:0]
}

func (s *sender) sendToWriter() {
	s.writerMut.Lock()

	s.sendBufMut.Lock()
	s.tmpBuf = append(s.tmpBuf, s.sendBuf...)
	s.sendBuf = clearCmdList(s.sendBuf)
	s.sendBufMut.Unlock()

	for _, cmd := range s.tmpBuf {
		_, err := s.writer.Write(cmd.data)
		if err != nil {
			panic(err)
		}
	}

	s.recv.push(s.tmpBuf)
	s.tmpBuf = clearCmdList(s.tmpBuf)

	s.writerMut.Unlock()
}

func (s *sender) publish(cmd *commandData) {
	var prevLen int

	s.sendBufMut.Lock()
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
