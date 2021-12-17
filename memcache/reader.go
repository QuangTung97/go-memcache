package memcache

type responseReader struct {
	data     []byte
	begin    uint64
	end      uint64
	dataMask uint64

	lastPos    uint64
	waitForEnd uint64
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

func (r *responseReader) findFirstNumberPos(start uint64, end uint64) uint64 {
	for pos := start; pos < end; pos++ {
		i := r.getIndex(pos)
		if r.data[i] >= '0' && r.data[i] <= '9' {
			return pos
		}
	}
	return end
}

func (r *responseReader) parseIntFrom(start uint64, end uint64) uint64 {
	result := uint64(0)

	start = r.findFirstNumberPos(start, end)
	for pos := start; pos < end; pos++ {
		i := r.getIndex(pos)
		if r.data[i] < '0' || r.data[i] > '9' {
			break
		}
		num := uint64(r.data[i] - '0')
		result *= 10
		result += num
	}

	return result
}

func (r *responseReader) isCRLF(pos uint64) bool {
	return r.data[r.getIndex(pos)] == '\r' && r.data[r.getIndex(pos+1)] == '\n'
}

func (r *responseReader) isVA(pos uint64) bool {
	return r.data[r.getIndex(pos)] == 'V' && r.data[r.getIndex(pos+1)] == 'A'
}

func (r *responseReader) returnIfHasFullResponseData() (int, bool) {
	if r.waitForEnd <= r.end {
		r.lastPos = r.waitForEnd
		r.waitForEnd = 0
		return int(r.lastPos - r.begin), true
	}
	return 0, false
}

func (r *responseReader) getNext() (int, bool) {
	if r.waitForEnd > 0 {
		return r.returnIfHasFullResponseData()
	}

	for pos := r.lastPos; pos+1 < r.end; pos++ {
		if !r.isCRLF(pos) {
			continue
		}

		r.lastPos = pos + 2
		if r.isVA(r.begin) {
			dataLen := r.parseIntFrom(r.begin+2, pos)
			r.waitForEnd = r.lastPos + dataLen + 2
			return r.returnIfHasFullResponseData()
		}
		return int(r.lastPos - r.begin), true
	}
	if r.end >= r.begin+2 {
		r.lastPos = r.end - 2
	}
	return 0, false
}

func (r *responseReader) readData(data []byte) {
	n := r.lastPos - r.begin
	first := r.getIndex(r.begin)
	max := r.getCap()

	copy(data, r.data[first:])
	if first+n > max {
		firstPart := max - first
		copy(data[firstPart:], r.data)
	}
	r.begin = r.lastPos
}
