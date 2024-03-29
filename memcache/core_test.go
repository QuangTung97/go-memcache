package memcache

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
)

func newCoreConnTest(
	writer FlushWriter, reader io.ReadCloser,
	closer io.Closer,
) *coreConnection {
	return newCoreConnection(netconn.NetConn{
		Writer: writer,
		Reader: reader,
		Closer: closer,
	}, computeOptions())
}

func TestCoreConnection_Read_Error_Partial_Result(t *testing.T) {
	writer1 := &FlushWriterMock{}
	reader1 := &readCloserInterfaceMock{}

	c := newCoreConnTest(writer1, reader1, reader1)

	writer1.WriteFunc = func(p []byte) (int, error) {
		return len(p), nil
	}
	writer1.FlushFunc = func() error { return nil }

	reader1.ReadFunc = func(p []byte) (int, error) {
		if len(reader1.ReadCalls()) == 1 {
			copy(p, "EN")
			return 2, nil
		}
		return 0, errors.New("some error")
	}
	reader1.CloseFunc = func() error { return nil }

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.publish(cmd1)

	cmd1.waitCompleted()

	assert.Equal(t, 2, len(reader1.ReadCalls()))
	assert.Equal(t, 1, len(reader1.CloseCalls()))
	assert.Equal(t, errors.New("some error"), cmd1.lastErr)

	cmd2 := newCommandFromString("mg key02 v\r\n")
	c.publish(cmd2)
	cmd2.waitCompleted()

	assert.Equal(t, errors.New("some error"), cmd2.lastErr)
	assert.GreaterOrEqual(t, len(reader1.ReadCalls()), 2)

	// AFTER Reset Connection

	c.waitForError()
	writer2 := &FlushWriterMock{}
	reader2 := &readCloserInterfaceMock{}
	c.resetNetConn(netconn.NetConn{
		Writer: writer2,
		Reader: reader2,
	})

	writer2.WriteFunc = func(p []byte) (int, error) {
		return len(p), nil
	}
	writer2.FlushFunc = func() error { return nil }

	reader2.ReadFunc = func(p []byte) (int, error) {
		data := "HD 100\r\n"
		copy(p, data)
		return len(data), nil
	}

	cmd3 := newCommandFromString("mg key03 v\r\n")
	c.publish(cmd3)
	cmd3.waitCompleted()

	assert.Equal(t, nil, cmd3.lastErr)
	assert.Equal(t, "HD 100\r\n", string(cmd3.responseData))
}

func TestCoreConnection_Continue_After_Read_Single_Command__Without_reader_Read_Again(t *testing.T) {
	writer1 := &FlushWriterMock{}
	reader1 := &readCloserInterfaceMock{}

	c := newCoreConnTest(writer1, reader1, reader1)

	writer1.WriteFunc = func(p []byte) (int, error) { return len(p), nil }
	writer1.FlushFunc = func() error { return nil }

	reader1.ReadFunc = func(p []byte) (int, error) {
		if len(reader1.ReadCalls()) > 1 {
			time.Sleep(24 * time.Hour)
		}
		data := "VA 4\r\nABCD\r\nHD\r\n"
		copy(p, data)
		return len(data), nil
	}

	cmd1 := newCommandFromString("mg key01 v\r\n")
	cmd2 := newCommandFromString("mg key02 v\r\n")
	c.publish(cmd1)
	c.publish(cmd2)

	cmd1.waitCompleted()
	cmd2.waitCompleted()

	assert.Equal(t, "VA 4\r\n", string(cmd1.responseData))
	assert.Equal(t, [][]byte{
		[]byte("ABCD"),
	}, cmd1.responseBinaries)
	assert.Equal(t, "HD\r\n", string(cmd2.responseData))
}

func TestCoreConnection_Reader_Returns_Incorrect_Data__Response_Reader_Returns_Error(t *testing.T) {
	writer1 := &FlushWriterMock{}
	reader1 := &readCloserInterfaceMock{}

	c := newCoreConnTest(writer1, reader1, reader1)

	finish := make(chan struct{})
	writer1.WriteFunc = func(p []byte) (int, error) {
		return len(p), nil
	}
	writer1.FlushFunc = func() error {
		close(finish)
		return nil
	}

	reader1.ReadFunc = func(p []byte) (int, error) {
		<-finish
		data := "VA x\r\n"
		copy(p, data)
		return len(data), nil
	}
	reader1.CloseFunc = func() error { return nil }

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.publish(cmd1)

	cmd1.waitCompleted()

	assert.Equal(t, ErrBrokenPipe{
		reason: "not a number after VA",
	}, cmd1.lastErr)

	assert.Equal(t, 1, len(writer1.WriteCalls()))
	assert.Equal(t, 1, len(writer1.FlushCalls()))

	assert.Equal(t, 1, len(reader1.ReadCalls()))
	assert.Equal(t, 1, len(reader1.CloseCalls()))

	c.sender.closeSendJob()
	c.waitReceiverShutdown()

	limiter := &c.sender.selector.writeLimiter
	assert.Equal(t, uint64(1), limiter.cmdWriteCount)
	assert.Equal(t, uint64(1), limiter.cmdReadCount.Load())
}

func TestCommandListReader(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		var buf bytes.Buffer
		s := newSender(newNetConnForTest(&buf), 8, 1000)
		t.Cleanup(func() {
			closeAndWaitSendJob(s)
		})

		r := newCmdListReader(s)

		cmd1 := newCommandFromString("mg key01 v\r\n")
		cmd2 := newCommandFromString("mg key02 v\r\n")

		s.publish(cmd1)
		s.publish(cmd2)

		n := r.readIfExhausted()
		assert.Equal(t, 2, n)

		assert.NotNil(t, r.cmdList[0])
		assert.NotNil(t, r.cmdList[1])
		assert.Nil(t, r.cmdList[2])

		r.next()

		assert.Nil(t, r.cmdList[0])
		assert.NotNil(t, r.cmdList[1])
		assert.Nil(t, r.cmdList[2])
	})
}
