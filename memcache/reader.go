package memcache

import (
	"bytes"
)

type readerState int

const (
	readerStateInit readerState = iota + 1
	readerStateCompleted
	readerStateFindLF

	readerStateFindA
	readerStateFindFirstNum
	readerStateGetNum
	readerStateFindCRForVA
	readerStateFindLFForVA
	readerStateReadBinaryData
)

type responseReader struct {
	currentCmd *commandData

	state   readerState
	tmpData []byte

	dataLen       uint64
	currentBinary []byte
	currentIndex  int

	lastErr error

	remainingData []byte
}

func newResponseReader() *responseReader {
	r := &responseReader{
		tmpData: make([]byte, 0, 64),
	}
	r.reset()
	return r
}

var errNoLFAfterCR = ErrBrokenPipe{reason: "no LF after CR"}

var errNotNumberAfterVA = ErrBrokenPipe{reason: "not a number after VA"}

func (r *responseReader) writeResponse(data []byte) {
	r.currentCmd.responseData = append(r.currentCmd.responseData, data...)
}

func (r *responseReader) setErrorAndReturn(err error) []byte {
	r.lastErr = err
	r.state = readerStateCompleted
	return nil
}

func (r *responseReader) setCompleted(data []byte, splitPoint int) []byte {
	r.state = readerStateCompleted
	r.writeResponse(data[:splitPoint])
	r.remainingData = data[splitPoint:]
	return nil
}

func (r *responseReader) processedSingleChar(data []byte, index int, newState readerState) []byte {
	r.writeResponse(data[:index+1])
	r.state = newState
	return data[index+1:]
}

func (r *responseReader) simpleContinue(data []byte) []byte {
	r.writeResponse(data)
	return nil
}

var respVAWithSpace = []byte("VA ")
var respVAWithSpaceLen = len(respVAWithSpace)

//revive:disable-next-line:cognitive-complexity
func (r *responseReader) handleStateInit(data []byte) []byte {
	for index, c := range data {
		if c == 'V' && index == 0 {
			if len(data) >= respVAWithSpaceLen+1 {
				ch := data[respVAWithSpaceLen]

				if bytes.Equal(data[:respVAWithSpaceLen], respVAWithSpace) && isDigit(ch) {
					r.resetTmpDataWithChar(ch)
					r.state = readerStateGetNum
					r.writeResponse(data[:respVAWithSpaceLen+1])
					return r.handleGetNum(data[respVAWithSpaceLen+1:])
				}
			}

			return r.processedSingleChar(data, index, readerStateFindA)
		}
		if c == '\r' {
			n := len(data)
			if index < n-1 && data[index+1] == '\n' {
				return r.setCompleted(data, index+2)
			}
			return r.processedSingleChar(data, index, readerStateFindLF)
		}
	}
	return r.simpleContinue(data)
}

func (r *responseReader) handleFindA(data []byte) []byte {
	if data[0] == 'A' {
		r.state = readerStateFindFirstNum
		r.writeResponse(data[:1])
		return data[1:]
	}
	r.state = readerStateInit
	return data
}

func (r *responseReader) resetTmpDataWithChar(c byte) {
	r.tmpData = r.tmpData[:0]
	r.tmpData = append(r.tmpData, c)
}

func (r *responseReader) handleFindFirstNum(data []byte) []byte {
	for index, c := range data {
		if c == ' ' {
			continue
		}

		if c >= '0' && c <= '9' {
			r.resetTmpDataWithChar(c)
			r.state = readerStateGetNum
			r.writeResponse(data[:index+1])
			return data[index+1:]
		}

		return r.setErrorAndReturn(errNotNumberAfterVA)
	}

	return r.simpleContinue(data)
}

func (r *responseReader) handleGetNum(data []byte) []byte {
	for index, c := range data {
		if c >= '0' && c <= '9' {
			r.tmpData = append(r.tmpData, c)
			continue
		}

		r.writeResponse(data[:index])

		n := len(r.tmpData)
		r.dataLen = uint64(r.tmpData[0] - '0')
		for i := 1; i < n; i++ {
			r.dataLen *= 10
			r.dataLen += uint64(r.tmpData[i] - '0')
		}

		r.currentBinary = make([]byte, r.dataLen)
		r.currentIndex = 0

		r.dataLen += 2 // CR + LF

		r.state = readerStateFindCRForVA
		return data[index:]
	}

	return r.simpleContinue(data)
}

func (r *responseReader) handleFindCRForVA(data []byte) []byte {
	for index, c := range data {
		if c == '\r' {
			n := len(data)

			if index < n-1 && data[index+1] == '\n' {
				r.state = readerStateReadBinaryData
				r.writeResponse(data[:index+2])
				return r.handleBinaryData(data[index+2:])
			}

			return r.processedSingleChar(data, index, readerStateFindLFForVA)
		}
	}
	return r.simpleContinue(data)
}

func (r *responseReader) handleFindLFForVA(data []byte) []byte {
	if data[0] != '\n' {
		return r.setErrorAndReturn(errNoLFAfterCR)
	}
	r.writeResponse(data[:1])
	r.state = readerStateReadBinaryData
	return data[1:]
}

func (r *responseReader) handleBinaryData(data []byte) []byte {
	n := uint64(len(data))
	if r.dataLen < n {
		n = r.dataLen
	}

	r.dataLen -= n

	if r.currentIndex < len(r.currentBinary) {
		copy(r.currentBinary[r.currentIndex:], data)
		r.currentIndex += int(n)
	}

	if r.dataLen == 0 {
		r.state = readerStateCompleted

		r.currentCmd.responseBinaries = append(r.currentCmd.responseBinaries, r.currentBinary)
		r.currentBinary = nil

		r.remainingData = data[n:]
		return nil
	}

	return data[n:]
}

func (r *responseReader) recvInLoop(data []byte) []byte {
	switch r.state {
	case readerStateInit:
		return r.handleStateInit(data)

	case readerStateFindA:
		return r.handleFindA(data)

	case readerStateFindFirstNum:
		return r.handleFindFirstNum(data)

	case readerStateGetNum:
		return r.handleGetNum(data)

	case readerStateFindCRForVA:
		return r.handleFindCRForVA(data)

	case readerStateFindLFForVA:
		return r.handleFindLFForVA(data)

	case readerStateReadBinaryData:
		return r.handleBinaryData(data)

	case readerStateFindLF:
		if data[0] != '\n' {
			return r.setErrorAndReturn(errNoLFAfterCR)
		}

		return r.setCompleted(data, 1)

	default:
		panic("invalid reader usage")
	}
}

func (r *responseReader) recv(data []byte) {
	for len(data) > 0 {
		data = r.recvInLoop(data)
	}
}

func (r *responseReader) setCurrentCommand(cmd *commandData) {
	r.currentCmd = cmd
}

func (r *responseReader) readNextData() bool {
	for {
		if r.state == readerStateCompleted {
			r.state = readerStateInit
			return true
		}

		if r.state == readerStateInit && len(r.remainingData) > 0 {
			remaining := r.remainingData
			r.remainingData = nil
			r.recv(remaining)
			continue
		}

		return false
	}
}

func (r *responseReader) hasError() error {
	return r.lastErr
}

func (r *responseReader) reset() {
	r.tmpData = r.tmpData[:]
	r.state = readerStateInit
	r.lastErr = nil
}
