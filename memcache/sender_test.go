package memcache

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newCommandFromString(s string) *commandData {
	cmd := newCommand()
	cmd.cmdCount = 1
	cmd.data = []byte(s)
	return cmd
}

func newNetConnForTest(buf *bytes.Buffer) netConn {
	return netConn{
		writer: NoopFlusher(buf),
		reader: nil,
		closer: nil,
	}
}

func TestSender_Publish(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 8)

	s.publish(newCommandFromString("mg key01 v\r\n"))
	s.publish(newCommandFromString("mg key02 v k\r\n"))

	assert.Equal(t, "mg key01 v\r\nmg key02 v k\r\n", buf.String())

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 2, n)
	assert.Equal(t, "mg key01 v\r\n", string(cmdList[0].data))
	assert.Equal(t, "mg key02 v k\r\n", string(cmdList[1].data))
}

func TestSender_Publish_Concurrent(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 8)

	s.ncMut.Lock()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()

		s.publish(newCommandFromString("mg key01\r\n"))
	}()

	go func() {
		defer wg.Done()

		s.publish(newCommandFromString("mg key02 v\r\n"))
	}()

	go func() {
		defer wg.Done()

		s.publish(newCommandFromString("mg key03 v\r\n"))
	}()

	time.Sleep(10 * time.Millisecond)

	s.ncMut.Unlock()
	wg.Wait()

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 3, n)

	result := map[string]struct{}{
		string(cmdList[0].data): {},
		string(cmdList[1].data): {},
		string(cmdList[2].data): {},
	}
	assert.Equal(t, map[string]struct{}{
		"mg key01\r\n":   {},
		"mg key02 v\r\n": {},
		"mg key03 v\r\n": {},
	}, result)
}

//revive:disable:cognitive-complexity
func TestSender_Publish_Stress_Test(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 2)
	assert.Equal(t, 4, s.send.maxLen)

	const numRounds = 200000

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()

		for i := 0; i < numRounds; i++ {
			key := fmt.Sprintf("A:KEY:%09d", i)
			s.publish(newCommandFromString(fmt.Sprintf("mg %s\r\n", key)))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numRounds; i++ {
			key := fmt.Sprintf("B:KEY:%09d", i)
			s.publish(newCommandFromString(fmt.Sprintf("mg %s\r\n", key)))
		}
	}()

	go func() {
		defer wg.Done()

		cmdList := make([]*commandData, 29)
		total := 0
		aKeys := 0
		bKeys := 0
		for total < 2*numRounds {
			n := s.readSentCommands(cmdList)
			for _, cmd := range cmdList[:n] {
				key := strings.Fields(string(cmd.data))[1]
				strList := strings.Split(key, ":")
				prefix := strList[0]
				num := strList[2]

				if prefix == "A" {
					if num != fmt.Sprintf("%09d", aKeys) {
						panic("Missing key")
					}
					aKeys++
				} else if prefix == "B" {
					if num != fmt.Sprintf("%09d", bKeys) {
						panic("Missing key")
					}
					bKeys++
				} else {
					panic("Invalid prefix")
				}
			}
			total += n
		}
		fmt.Println(aKeys, bKeys)
	}()

	wg.Wait()
}

//revive:enable:cognitive-complexity

func TestSender_Publish_Wait_Not_Ended_On_Fresh_Start(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 8)

	var ended uint32
	go func() {
		s.waitForError()
		atomic.StoreUint32(&ended, 1)
	}()

	time.Sleep(5 * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&ended))
}

func TestSender_Publish_Write_Error(t *testing.T) {
	writer := &FlushWriterMock{}
	closer := &closerInterfaceMock{}
	s := newSender(netConn{writer: writer, closer: closer}, 8)

	writer.WriteFunc = func(p []byte) (int, error) {
		return 0, errors.New("some error")
	}
	closer.CloseFunc = func() error {
		return nil
	}

	cmd1 := newCommandFromString("mg key01 v\r\n")
	cmd2 := newCommandFromString("mg key02 v\r\n")

	s.publish(cmd1)
	s.publish(cmd2)

	assert.Equal(t, 1, len(writer.WriteCalls()))
	assert.Equal(t, 1, len(closer.CloseCalls()))

	cmd1.waitCompleted()
	cmd2.waitCompleted()

	assert.Equal(t, errors.New("some error"), cmd1.lastErr)
	assert.Equal(t, errors.New("some error"), cmd2.lastErr)

	s.waitForError()
}

func TestSender_Publish_Flush_Error(t *testing.T) {
	writer := &FlushWriterMock{}
	closer := &closerInterfaceMock{}
	s := newSender(netConn{writer: writer, closer: closer}, 8)

	writer.WriteFunc = func(p []byte) (int, error) {
		return len(p), nil
	}
	writer.FlushFunc = func() error {
		return errors.New("some error")
	}
	closer.CloseFunc = func() error {
		return nil
	}

	cmd1 := newCommandFromString("mg key01 v\r\n")
	cmd2 := newCommandFromString("mg key02 v\r\n")

	s.publish(cmd1)
	s.publish(cmd2)

	assert.Equal(t, 1, len(writer.WriteCalls()))
	assert.Equal(t, 1, len(closer.CloseCalls()))

	cmd1.waitCompleted()
	cmd2.waitCompleted()

	assert.Equal(t, errors.New("some error"), cmd1.lastErr)
	assert.Equal(t, errors.New("some error"), cmd2.lastErr)

	assert.Equal(t, false, cmd1.resetReader)
	assert.Equal(t, false, cmd1.resetReader)

	s.waitForError()
}

func TestSender_Publish_Write_Error_Then_ResetConn(t *testing.T) {
	writer1 := &FlushWriterMock{}
	writer2 := &FlushWriterMock{}

	reader2 := &readCloserInterfaceMock{}

	closer := &closerInterfaceMock{}
	s := newSender(netConn{writer: writer1, closer: closer}, 8)

	writer1.WriteFunc = func(p []byte) (int, error) {
		return 0, errors.New("some error")
	}
	closer.CloseFunc = func() error { return nil }

	cmd1 := newCommandFromString("mg key01 v\r\n")
	s.publish(cmd1)
	cmd1.waitCompleted()
	assert.Equal(t, false, cmd1.resetReader)

	s.waitForError()
	s.resetNetConn(netConn{writer: writer2, reader: reader2, closer: closer})

	writer2.WriteFunc = func(p []byte) (int, error) {
		return len(p), nil
	}
	writer2.FlushFunc = func() error { return nil }

	cmd2 := newCommandFromString("mg key02 v\r\n")
	s.publish(cmd2)

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte("mg key02 v\r\n"), cmdList[0].data)
	assert.Same(t, reader2, cmdList[0].reader)
	assert.Equal(t, true, cmdList[0].resetReader)
}

func TestSendBuffer(t *testing.T) {
	var b sendBuffer
	initSendBuffer(&b, 1)

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		b.push(newCommandFromString("mg key01 v\r\n"))
	}()
	go func() {
		defer wg.Done()
		b.push(newCommandFromString("mg key02 v\r\n"))
	}()
	go func() {
		defer wg.Done()
		b.push(newCommandFromString("mg key03 v\r\n"))
	}()
	go func() {
		defer wg.Done()
		b.push(newCommandFromString("mg key04 v\r\n"))
	}()

	time.Sleep(5 * time.Millisecond)

	commands := map[string]struct{}{}

	output := b.popAll(nil)
	assert.Equal(t, 2, len(output))
	commands[string(output[0].data)] = struct{}{}
	commands[string(output[1].data)] = struct{}{}

	wg.Wait()

	output = b.popAll(nil)
	assert.Equal(t, 2, len(output))
	commands[string(output[0].data)] = struct{}{}
	commands[string(output[1].data)] = struct{}{}

	assert.Equal(t, map[string]struct{}{
		"mg key01 v\r\n": {},
		"mg key02 v\r\n": {},
		"mg key03 v\r\n": {},
		"mg key04 v\r\n": {},
	}, commands)
}
