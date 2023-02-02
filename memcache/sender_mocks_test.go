// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package memcache

import (
	"sync"
)

// Ensure, that FlushWriterMock does implement FlushWriter.
// If this is not the case, regenerate this file with moq.
var _ FlushWriter = &FlushWriterMock{}

// FlushWriterMock is a mock implementation of FlushWriter.
//
//	func TestSomethingThatUsesFlushWriter(t *testing.T) {
//
//		// make and configure a mocked FlushWriter
//		mockedFlushWriter := &FlushWriterMock{
//			FlushFunc: func() error {
//				panic("mock out the Flush method")
//			},
//			WriteFunc: func(p []byte) (int, error) {
//				panic("mock out the Write method")
//			},
//		}
//
//		// use mockedFlushWriter in code that requires FlushWriter
//		// and then make assertions.
//
//	}
type FlushWriterMock struct {
	// FlushFunc mocks the Flush method.
	FlushFunc func() error

	// WriteFunc mocks the Write method.
	WriteFunc func(p []byte) (int, error)

	// calls tracks calls to the methods.
	calls struct {
		// Flush holds details about calls to the Flush method.
		Flush []struct {
		}
		// Write holds details about calls to the Write method.
		Write []struct {
			// P is the p argument value.
			P []byte
		}
	}
	lockFlush sync.RWMutex
	lockWrite sync.RWMutex
}

// Flush calls FlushFunc.
func (mock *FlushWriterMock) Flush() error {
	if mock.FlushFunc == nil {
		panic("FlushWriterMock.FlushFunc: method is nil but FlushWriter.Flush was just called")
	}
	callInfo := struct {
	}{}
	mock.lockFlush.Lock()
	mock.calls.Flush = append(mock.calls.Flush, callInfo)
	mock.lockFlush.Unlock()
	return mock.FlushFunc()
}

// FlushCalls gets all the calls that were made to Flush.
// Check the length with:
//
//	len(mockedFlushWriter.FlushCalls())
func (mock *FlushWriterMock) FlushCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockFlush.RLock()
	calls = mock.calls.Flush
	mock.lockFlush.RUnlock()
	return calls
}

// Write calls WriteFunc.
func (mock *FlushWriterMock) Write(p []byte) (int, error) {
	if mock.WriteFunc == nil {
		panic("FlushWriterMock.WriteFunc: method is nil but FlushWriter.Write was just called")
	}
	callInfo := struct {
		P []byte
	}{
		P: p,
	}
	mock.lockWrite.Lock()
	mock.calls.Write = append(mock.calls.Write, callInfo)
	mock.lockWrite.Unlock()
	return mock.WriteFunc(p)
}

// WriteCalls gets all the calls that were made to Write.
// Check the length with:
//
//	len(mockedFlushWriter.WriteCalls())
func (mock *FlushWriterMock) WriteCalls() []struct {
	P []byte
} {
	var calls []struct {
		P []byte
	}
	mock.lockWrite.RLock()
	calls = mock.calls.Write
	mock.lockWrite.RUnlock()
	return calls
}

// Ensure, that closerInterfaceMock does implement closerInterface.
// If this is not the case, regenerate this file with moq.
var _ closerInterface = &closerInterfaceMock{}

// closerInterfaceMock is a mock implementation of closerInterface.
//
//	func TestSomethingThatUsescloserInterface(t *testing.T) {
//
//		// make and configure a mocked closerInterface
//		mockedcloserInterface := &closerInterfaceMock{
//			CloseFunc: func() error {
//				panic("mock out the Close method")
//			},
//		}
//
//		// use mockedcloserInterface in code that requires closerInterface
//		// and then make assertions.
//
//	}
type closerInterfaceMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
	}
	lockClose sync.RWMutex
}

// Close calls CloseFunc.
func (mock *closerInterfaceMock) Close() error {
	if mock.CloseFunc == nil {
		panic("closerInterfaceMock.CloseFunc: method is nil but closerInterface.Close was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//
//	len(mockedcloserInterface.CloseCalls())
func (mock *closerInterfaceMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Ensure, that readCloserInterfaceMock does implement readCloserInterface.
// If this is not the case, regenerate this file with moq.
var _ readCloserInterface = &readCloserInterfaceMock{}

// readCloserInterfaceMock is a mock implementation of readCloserInterface.
//
//	func TestSomethingThatUsesreadCloserInterface(t *testing.T) {
//
//		// make and configure a mocked readCloserInterface
//		mockedreadCloserInterface := &readCloserInterfaceMock{
//			CloseFunc: func() error {
//				panic("mock out the Close method")
//			},
//			ReadFunc: func(p []byte) (int, error) {
//				panic("mock out the Read method")
//			},
//		}
//
//		// use mockedreadCloserInterface in code that requires readCloserInterface
//		// and then make assertions.
//
//	}
type readCloserInterfaceMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// ReadFunc mocks the Read method.
	ReadFunc func(p []byte) (int, error)

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// Read holds details about calls to the Read method.
		Read []struct {
			// P is the p argument value.
			P []byte
		}
	}
	lockClose sync.RWMutex
	lockRead  sync.RWMutex
}

// Close calls CloseFunc.
func (mock *readCloserInterfaceMock) Close() error {
	if mock.CloseFunc == nil {
		panic("readCloserInterfaceMock.CloseFunc: method is nil but readCloserInterface.Close was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//
//	len(mockedreadCloserInterface.CloseCalls())
func (mock *readCloserInterfaceMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Read calls ReadFunc.
func (mock *readCloserInterfaceMock) Read(p []byte) (int, error) {
	if mock.ReadFunc == nil {
		panic("readCloserInterfaceMock.ReadFunc: method is nil but readCloserInterface.Read was just called")
	}
	callInfo := struct {
		P []byte
	}{
		P: p,
	}
	mock.lockRead.Lock()
	mock.calls.Read = append(mock.calls.Read, callInfo)
	mock.lockRead.Unlock()
	return mock.ReadFunc(p)
}

// ReadCalls gets all the calls that were made to Read.
// Check the length with:
//
//	len(mockedreadCloserInterface.ReadCalls())
func (mock *readCloserInterfaceMock) ReadCalls() []struct {
	P []byte
} {
	var calls []struct {
		P []byte
	}
	mock.lockRead.RLock()
	calls = mock.calls.Read
	mock.lockRead.RUnlock()
	return calls
}
