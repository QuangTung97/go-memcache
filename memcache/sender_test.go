package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/QuangTung97/go-memcache/memcache/netconn"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newCommandFromString(s string) *commandData {
	cmd := newCommand()
	cmd.cmdCount = 1
	cmd.requestData = []byte(s)
	return cmd
}

func newNetConnForTest(buf *bytes.Buffer) netconn.NetConn {
	return netconn.NetConn{
		Writer: netconn.NoopFlusher(buf),
		Reader: nil,
		Closer: nil,
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
	assert.Equal(t, "", string(cmdList[0].responseData))
	assert.Equal(t, "", string(cmdList[1].responseData))
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

	assert.Equal(t, map[string]struct{}{
		"mg key01":   {},
		"mg key02 v": {},
		"mg key03 v": {},
	}, scanBufferToLines(&buf))
}

func scanBufferToLines(read io.Reader) map[string]struct{} {
	var lines []string

	scanner := bufio.NewScanner(read)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}

	return stringsToMap(lines)
}

func stringsToMap(list []string) map[string]struct{} {
	result := map[string]struct{}{}
	for _, s := range list {
		result[s] = struct{}{}
	}
	return result
}

//revive:disable-next-line:cognitive-complexity
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
		for total < 2*numRounds {
			n := s.readSentCommands(cmdList)
			total += n
		}
	}()

	wg.Wait()

	aKeys := 0
	bKeys := 0

	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		line := scanner.Text()

		key := strings.Fields(line)[1]
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

	assert.Equal(t, numRounds, aKeys)
	assert.Equal(t, numRounds, bKeys)
}

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
	s := newSender(netconn.NetConn{Writer: writer, Closer: closer}, 8)

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

	assert.Equal(t, errors.New("some error"), cmd1.lastErr)
	assert.Equal(t, errors.New("some error"), cmd2.lastErr)

	s.waitForError()
}

func TestSender_Publish_Flush_Error(t *testing.T) {
	writer := &FlushWriterMock{}
	closer := &closerInterfaceMock{}
	s := newSender(netconn.NetConn{Writer: writer, Closer: closer}, 8)

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

	assert.Equal(t, errors.New("some error"), cmd1.lastErr)
	assert.Equal(t, errors.New("some error"), cmd2.lastErr)

	s.waitForError()
}

func TestSender_Publish_Write_Error_Then_ResetConn(t *testing.T) {
	writer1 := &FlushWriterMock{}
	writer2 := &FlushWriterMock{}

	reader2 := &readCloserInterfaceMock{}

	closer := &closerInterfaceMock{}
	s := newSender(netconn.NetConn{Writer: writer1, Closer: closer}, 8)

	var writeBytes []byte
	writer1.WriteFunc = func(p []byte) (int, error) {
		writeBytes = append(writeBytes, p...)
		return 0, errors.New("some error")
	}
	closer.CloseFunc = func() error { return nil }

	cmd1 := newCommandFromString("mg key01 v\r\n")

	s.publish(cmd1)

	s.waitForError()
	s.resetNetConn(netconn.NetConn{Writer: writer2, Reader: reader2, Closer: closer})

	writer2.WriteFunc = func(p []byte) (int, error) {
		return len(p), nil
	}
	writer2.FlushFunc = func() error { return nil }

	cmd2 := newCommandFromString("mg key02 v\r\n")
	s.publish(cmd2)

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 2, n)
	assert.Equal(t, []byte("mg key01 v\r\n"), writeBytes)
	assert.Nil(t, cmdList[0].reader)
	assert.Same(t, reader2, cmdList[1].reader)
}

func TestSendBuffer_Concurrent_With_Waiting(t *testing.T) {
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
	commands[string(output[0].requestData)] = struct{}{}
	commands[string(output[1].requestData)] = struct{}{}

	wg.Wait()

	output = b.popAll(nil)
	assert.Equal(t, 2, len(output))
	commands[string(output[0].requestData)] = struct{}{}
	commands[string(output[1].requestData)] = struct{}{}

	assert.Equal(t, map[string]struct{}{
		"mg key01 v\r\n": {},
		"mg key02 v\r\n": {},
		"mg key03 v\r\n": {},
		"mg key04 v\r\n": {},
	}, commands)
}

func TestRecvBuffer__Push_When_Closed(t *testing.T) {
	var b recvBuffer
	initRecvBuffer(&b, 1)

	remaining, closed := b.push([]*commandData{
		newCommandFromString("mg key01 v\r\n"),
		newCommandFromString("mg key02 v\r\n"),
	})
	assert.Equal(t, false, closed)
	assert.Equal(t, 0, len(remaining))

	b.closeBuffer()

	remaining, closed = b.push([]*commandData{
		newCommandFromString("mg key03 v\r\n"),
	})
	assert.Equal(t, true, closed)
	assert.Equal(t, []*commandData{newCommandFromString("mg key03 v\r\n")}, remaining)

	cmdList := make([]*commandData, 1024)
	size := b.read(cmdList)
	assert.Equal(t, 2, size)
}

func TestRecvBuffer__Push_When_Closed__Concurrently_When_Reach_Limit(t *testing.T) {
	var b recvBuffer
	initRecvBuffer(&b, 1)

	remaining, closed := b.push([]*commandData{
		newCommandFromString("mg key01 v\r\n"),
	})
	assert.Equal(t, false, closed)
	assert.Equal(t, 0, len(remaining))

	cmdList := make([]*commandData, 1024)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(20 * time.Millisecond)
		b.closeBuffer()
	}()

	remaining, closed = b.push([]*commandData{
		newCommandFromString("mg key02 v\r\n"),
		newCommandFromString("mg key03 v\r\n"),
	})

	assert.Equal(t, true, closed)
	assert.Equal(t, []*commandData{
		newCommandFromString("mg key03 v\r\n"),
	}, remaining)

	wg.Wait()

	size := b.read(cmdList)
	assert.Equal(t, 2, size)
	assert.Equal(t, []*commandData{
		newCommandFromString("mg key01 v\r\n"),
		newCommandFromString("mg key02 v\r\n"),
	}, cmdList[:2])
}
