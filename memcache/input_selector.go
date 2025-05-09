package memcache

import (
	"sync"
	"sync/atomic"
)

// inputSelector is for avoiding starvation.
// For example, if a big pipeline with 100_000 mg ops followed by a small pipeline of 10 ops.
// Then it will try to send small pipeline after **writeLimiter.writeLimit** before continuing sending the big pipeline.
type inputSelector struct {
	sendBuf      *sendBuffer
	writeLimiter connWriteLimiter

	inputList   selectorCommandList
	longCmdList selectorCommandList

	readLimit int
}

// selectorCommandList is a singly-linked list. With:
// - head: head of the linked-list
// - last: address of last element's next pointer.
// The **last** field is used to help append to the linked-list in O(1) time.
type selectorCommandList struct {
	head *commandListData
	last **commandListData
}

func initSelectorCommandList(l *selectorCommandList) {
	l.head = nil
	l.last = &l.head
}

func (l *selectorCommandList) append(inputCmd *commandListData) {
	*l.last = inputCmd

	// traverse the next.link and update the **last** pointer
	next := inputCmd
	for next != nil {
		l.last = &next.link
		next = next.link
	}
}

func (l *selectorCommandList) removeFirst() {
	l.head = l.head.link
	if l.head == nil {
		l.last = &l.head
	}
}

func initInputSelector(
	s *inputSelector,
	sendBuf *sendBuffer,
	writeCmdLimit int,
	readLimit int,
) {
	s.sendBuf = sendBuf
	initConnWriteLimiter(&s.writeLimiter, writeCmdLimit)

	initSelectorCommandList(&s.inputList)
	initSelectorCommandList(&s.longCmdList)

	s.readLimit = readLimit
}

type getNextCommandStatus struct {
	allowMore  bool
	hasSibling bool
}

func (s *inputSelector) getNextCommand(
	cmdList *selectorCommandList, result []*commandListData, justWaited *bool,
) (newResult []*commandListData, status getNextCommandStatus) {
	if cmdList.head == nil {
		return result, getNextCommandStatus{
			allowMore: true,
		}
	}

	cmd := cmdList.head
	writeCount := uint64(cmd.cmdCount)

	if len(result) >= s.readLimit {
		return result, getNextCommandStatus{
			allowMore: false,
		}
	}

	if writeCount <= s.writeLimiter.writeLimit {
		doWait := len(result) == 0
		if !s.writeLimiter.allowMoreWrite(writeCount, doWait) {
			return result, getNextCommandStatus{
				allowMore: false,
			}
		}
		if s.writeLimiter.justWaited {
			*justWaited = true
		}
	}

	s.writeLimiter.addWriteCount(writeCount)

	cmdList.removeFirst()
	cmd.link = nil

	result = append(result, cmd)

	var hasSibling bool
	if cmd.sibling != nil {
		hasSibling = true
		sibling := cmd.sibling
		s.longCmdList.append(sibling)
	}

	return result, getNextCommandStatus{
		allowMore:  true,
		hasSibling: hasSibling,
	}
}

func (s *inputSelector) isEmpty() bool {
	return s.inputList.head == nil && s.longCmdList.head == nil
}

type popAllStatus struct {
	justWaited bool
	closed     bool
}

func (s *inputSelector) popAllThenRead(result []*commandListData) ([]*commandListData, popAllStatus) {
	popWaiting := s.isEmpty() && len(result) == 0
	inputCmdList, closed := s.sendBuf.popAll(popWaiting)

	s.inputList.append(inputCmdList)

	var justWaited bool
	var status getNextCommandStatus
	for !s.isEmpty() {
		result, status = s.getNextCommand(&s.inputList, result, &justWaited)
		if !status.allowMore {
			return result, popAllStatus{}
		}
		if status.hasSibling {
			continue
		}

		result, status = s.getNextCommand(&s.longCmdList, result, &justWaited)
		if !status.allowMore {
			return result, popAllStatus{}
		}
	}

	return result, popAllStatus{
		closed:     closed,
		justWaited: justWaited,
	}
}

func (s *inputSelector) readCommands(placeholder []*commandListData) ([]*commandListData, bool) {
	result := placeholder
	for {
		var status popAllStatus
		result, status = s.popAllThenRead(result)
		if status.justWaited {
			continue
		}
		return result, status.closed
	}
}

func (s *inputSelector) addReadCount(num uint64) {
	s.writeLimiter.addReadCount(num)
}

// ==================================
// Conn Write Limiter
// ==================================

func initConnWriteLimiter(l *connWriteLimiter, limit int) {
	l.writeLimit = uint64(limit)
	l.readCond = sync.NewCond(&l.readMut)
}

type connWriteLimiter struct {
	writeLimit uint64

	cmdWriteCount uint64

	readMut      sync.Mutex
	readCond     *sync.Cond
	cmdReadCount atomic.Uint64

	justWaited bool
}

func (l *connWriteLimiter) addWriteCount(num uint64) {
	l.cmdWriteCount += num
}

//revive:disable-next-line:flag-parameter
func (l *connWriteLimiter) allowMoreWrite(num uint64, waiting bool) bool {
	l.justWaited = false
	newWriteCount := l.cmdWriteCount + num

	if newWriteCount <= l.cmdReadCount.Load()+l.writeLimit {
		return true
	}
	if !waiting {
		return false
	}

	l.readMut.Lock()
	for newWriteCount > l.cmdReadCount.Load()+l.writeLimit {
		l.justWaited = true
		l.readCond.Wait()
	}
	l.readMut.Unlock()

	return true
}

func (l *connWriteLimiter) addReadCount(num uint64) {
	l.readMut.Lock()
	l.cmdReadCount.Add(num)
	l.readMut.Unlock()
	l.readCond.Signal()
}
