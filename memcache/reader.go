package memcache

type responseReader struct {
	data      []byte
	size      int
	lastIndex int

	valid  bool
	cmdEnd int
}

func newResponseReader() *responseReader {
	return &responseReader{
		data:      make([]byte, 1<<21),
		lastIndex: 0,
	}
}

func (r *responseReader) recv(data []byte) {
	copy(r.data, data)
	r.size += len(data)

	for i := r.lastIndex; i < r.size-1; i++ {
		if r.data[i] == '\r' && r.data[i+1] == '\n' {
			r.valid = true
			r.cmdEnd = i + 2
			break
		}
	}
}

func (r *responseReader) hasNext() bool {
	return r.valid
}

func (r *responseReader) responseSize() int {
	return r.cmdEnd
}

func (r *responseReader) readData(data []byte) {
	copy(data, r.data)
}
