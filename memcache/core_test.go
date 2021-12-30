package memcache

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCoreConnection_Read_Error_Partial_Result(t *testing.T) {
	writer1 := &FlushWriterMock{}
	reader1 := &readCloserInterfaceMock{}

	c := newCoreConnection(netConn{
		writer: writer1,
		reader: reader1,
	})

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
	assert.Equal(t, 2, len(reader1.ReadCalls()))

	// AFTER Reset Connection

	c.waitForError()
	writer2 := &FlushWriterMock{}
	reader2 := &readCloserInterfaceMock{}
	c.resetNetConn(netConn{
		writer: writer2,
		reader: reader2,
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
	assert.Equal(t, "HD 100\r\n", string(cmd3.data))
}

func TestCoreConnection_Continue_After_Read_Single_Command__Without_reader_Read_Again(t *testing.T) {
	writer1 := &FlushWriterMock{}
	reader1 := &readCloserInterfaceMock{}

	c := newCoreConnection(netConn{
		writer: writer1,
		reader: reader1,
	})

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

	assert.Equal(t, "VA 4\r\nABCD\r\n", string(cmd1.data))
	assert.Equal(t, "HD\r\n", string(cmd2.data))
}
