package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
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
	s := newSender(newNetConnForTest(&buf), 8, 1000)

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
	s := newSender(newNetConnForTest(&buf), 8, 1000)

	s.connMut.Lock()

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

	s.connMut.Unlock()
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
	s := newSender(newNetConnForTest(&buf), 2, 1000)

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
	s := newSender(newNetConnForTest(&buf), 8, 1000)

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

	s := newSender(netconn.NetConn{Writer: writer, Closer: closer}, 8, 1000)

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

	assert.Equal(t, errors.New("some error"), cmd1.conn.getLastErrorInternal())
	assert.Equal(t, errors.New("some error"), cmd2.conn.getLastErrorInternal())

	s.waitForError()
}

func TestSender_Publish_Flush_Error(t *testing.T) {
	writer := &FlushWriterMock{}
	closer := &closerInterfaceMock{}
	s := newSender(netconn.NetConn{Writer: writer, Closer: closer}, 8, 1000)

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

	assert.Equal(t, errors.New("some error"), cmd1.conn.getLastErrorInternal())
	assert.Equal(t, errors.New("some error"), cmd2.conn.getLastErrorInternal())

	s.waitForError()
}

func TestSender_Publish_Write_Error_Then_ResetConn(t *testing.T) {
	writer1 := &FlushWriterMock{}
	writer2 := &FlushWriterMock{}

	reader2 := &readCloserInterfaceMock{}

	closer := &closerInterfaceMock{}
	s := newSender(netconn.NetConn{Writer: writer1, Closer: closer}, 8, 1000)

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

	assert.Nil(t, cmdList[0].conn.reader)
	assert.Same(t, reader2, cmdList[1].conn.reader)
}

func TestSender_ResetConn_After_Shutdown__Should_Close_Conn(t *testing.T) {
	closer1 := &closerInterfaceMock{}
	closer1.CloseFunc = func() error {
		return nil
	}

	s := newSender(netconn.NetConn{Writer: nil, Reader: nil, Closer: closer1}, 8, 1000)

	s.shutdown()

	assert.Equal(t, 1, len(closer1.CloseCalls()))

	// Do Reset
	closer2 := &closerInterfaceMock{}
	closer2.CloseFunc = func() error {
		return nil
	}
	s.resetNetConn(netconn.NetConn{Writer: nil, Reader: nil, Closer: closer2})

	assert.Equal(t, 1, len(closer2.CloseCalls()))
	assert.Equal(t, ErrConnClosed, s.conn.getLastErrorInternal())
}

func TestRecvBuffer__Push_And_Then_Read__Command_List_Should_Be_Nil(t *testing.T) {
	var b recvBuffer
	initRecvBuffer(&b, 2)

	remaining, closed := b.push([]*commandData{
		newCommandFromString("mg key01 v\r\n"),
		newCommandFromString("mg key02 v\r\n"),
		newCommandFromString("mg key02 v\r\n"),
	})
	assert.Equal(t, false, closed)
	assert.Equal(t, 0, len(remaining))

	cmdList := make([]*commandData, 1024)
	size := b.read(cmdList)
	assert.Equal(t, 3, size)

	assert.Nil(t, b.buf[0])
	assert.Nil(t, b.buf[1])
	assert.Nil(t, b.buf[2])
	assert.Nil(t, b.buf[3])
	assert.Equal(t, 4, len(b.buf))

	remaining, closed = b.push([]*commandData{
		newCommandFromString("mg key03 v\r\n"),
		newCommandFromString("mg key04 v\r\n"),
	})
	assert.Equal(t, false, closed)
	assert.Equal(t, 0, len(remaining))

	// Read Again
	cmdList = make([]*commandData, 1024)
	size = b.read(cmdList)
	assert.Equal(t, 2, size)

	assert.Nil(t, b.buf[0])
	assert.Nil(t, b.buf[1])
	assert.Nil(t, b.buf[2])
	assert.Nil(t, b.buf[3])
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

	cmd3 := newCommandFromString("mg key03 v\r\n")

	remaining, closed = b.push([]*commandData{cmd3})
	assert.Equal(t, true, closed)
	assert.Equal(t, []*commandData{cmd3}, remaining)

	cmdList := make([]*commandData, 1024)
	size := b.read(cmdList)
	assert.Equal(t, 2, size)
}

func TestRecvBuffer__Push_When_Closed__Concurrently_When_Reach_Limit(t *testing.T) {
	var b recvBuffer
	initRecvBuffer(&b, 1)

	cmd1 := newCommandFromString("mg key01 v\r\n")

	remaining, closed := b.push([]*commandData{cmd1})
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

	cmd2 := newCommandFromString("mg key02 v\r\n")
	cmd3 := newCommandFromString("mg key03 v\r\n")

	remaining, closed = b.push([]*commandData{cmd2, cmd3})

	assert.Equal(t, true, closed)
	assert.Equal(t, []*commandData{cmd3}, remaining)

	wg.Wait()

	size := b.read(cmdList)
	assert.Equal(t, 2, size)
	assert.Equal(t, []*commandData{
		cmd1, cmd2,
	}, cmdList[:2])
}
