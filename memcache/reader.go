package memcache

type responseReader struct {
	data     []byte
	begin    uint64
	end      uint64
	dataMask uint64

	lastPos    uint64
	waitForEnd uint64

	lastErr error
}

func newResponseReader(sizeLog int) *responseReader {
	return &responseReader{
		data:     make([]byte, 1<<sizeLog),
		begin:    0,
		end:      0,
		dataMask: 1<<sizeLog - 1,

		lastPos: 0,
	}
}

func (r *responseReader) getCap() uint64 {
	return uint64(len(r.data))
}

func (r *responseReader) recv(data []byte) {
	first := r.getIndex(r.end)
	copy(r.data[first:], data)

	max := r.getCap()
	if first+uint64(len(data)) > max {
		firstPart := max - first
		copy(r.data, data[firstPart:])
	}

	r.end += uint64(len(data))
}

func (r *responseReader) getIndex(pos uint64) uint64 {
	return pos & r.dataMask
}

func (r *responseReader) findFirstNumberPos(start uint64, end uint64) (uint64, error) {
	for pos := start; pos < end; pos++ {
		i := r.getIndex(pos)
		if r.data[i] >= '0' && r.data[i] <= '9' {
			return pos, nil
		}
	}
	return end, ErrBrokenPipe{reason: "not a number after VA"}
}

func (r *responseReader) parseIntFrom(start uint64, end uint64) (uint64, error) {
	result := uint64(0)

	start, err := r.findFirstNumberPos(start, end)
	if err != nil {
		return 0, err
	}
	for pos := start; pos < end; pos++ {
		i := r.getIndex(pos)
		if r.data[i] < '0' || r.data[i] > '9' {
			break
		}
		num := uint64(r.data[i] - '0')
		result *= 10
		result += num
	}

	return result, nil
}

func (r *responseReader) isCRLF(pos uint64) bool {
	return r.data[r.getIndex(pos)] == '\r' && r.data[r.getIndex(pos+1)] == '\n'
}

func (r *responseReader) isVA(pos uint64) bool {
	return r.data[r.getIndex(pos)] == 'V' && r.data[r.getIndex(pos+1)] == 'A'
}

func (r *responseReader) returnIfHasFullResponseData() bool {
	if r.waitForEnd <= r.end {
		r.lastPos = r.waitForEnd
		r.waitForEnd = 0
		return true
	}
	return false
}

func (r *responseReader) hasNext() bool {
	if r.waitForEnd > 0 {
		return r.returnIfHasFullResponseData()
	}

	for pos := r.lastPos; pos+1 < r.end; pos++ {
		if !r.isCRLF(pos) {
			continue
		}

		r.lastPos = pos + 2
		if r.isVA(r.begin) {
			dataLen, err := r.parseIntFrom(r.begin+2, pos)
			if err != nil {
				r.lastErr = err
				return false
			}
			r.waitForEnd = r.lastPos + dataLen + 2
			return r.returnIfHasFullResponseData()
		}
		return true
	}
	if r.end >= r.begin+2 {
		r.lastPos = r.end - 2
	}
	return false
}

func (r *responseReader) readData(data []byte) []byte {
	n := r.lastPos - r.begin
	first := r.getIndex(r.begin)
	max := r.getCap()

	firstEnd := first + n
	if firstEnd > max {
		firstEnd = max
	}

	data = append(data, r.data[first:firstEnd]...)

	if firstEnd == max {
		firstPart := max - first
		secondPart := n - firstPart
		data = append(data, r.data[:secondPart]...)
	}

	r.begin = r.lastPos

	return data
}

func (r *responseReader) hasError() error {
	return r.lastErr
}

func (r *responseReader) reset() {
	r.begin = 0
	r.end = 0
	r.lastPos = 0
	r.waitForEnd = 0
	r.lastErr = nil
}
