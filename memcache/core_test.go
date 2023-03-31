package memcache

import (
	"errors"
	"github.com/QuangTung97/go-memcache/memcache/netconn"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
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

	assert.Equal(t, "VA 4\r\nABCD\r\n", string(cmd1.responseData))
	assert.Equal(t, "HD\r\n", string(cmd2.responseData))
}
