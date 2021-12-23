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

func newSender(writer io.Writer, bufSizeLog int) *sender {
	maxSize := 1 << bufSizeLog
	s := &sender{
		writer:     writer,
		sendBuf:    make([]*commandData, 0, maxSize),
		sendBufMax: maxSize,
		recv:       newRecvBuffer(bufSizeLog),
	}
	s.sendFullCond = sync.NewCond(&s.sendBufMut)
	return s
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

	s.sendFullCond.Signal()

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

func (s *sender) publish(cmd ...*commandData) {
	var prevLen int

	s.sendBufMut.Lock()
	for len(s.sendBuf) >= s.sendBufMax {
		s.sendFullCond.Wait()
	}

	prevLen = len(s.sendBuf)
	s.sendBuf = append(s.sendBuf, cmd...)
	s.sendBufMut.Unlock()

	if prevLen > 0 {
		return
	}

	s.sendToWriter()
}

func (s *sender) readSentCommands(cmdList []*commandData) int {
	return s.recv.read(cmdList)
}
