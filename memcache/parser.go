package memcache

type parser struct {
	data []byte
}

func initParser(p *parser, data []byte) {
	p.data = data
}

type mgetResponseType int

const (
	mgetResponseTypeVA mgetResponseType = iota + 1
	mgetResponseTypeHD
	mgetResponseTypeEN
)

type mgetResponse struct {
	responseType mgetResponseType
	size         uint32
	data         []byte
}

var errInvalidMGet = ErrBrokenPipe{reason: "can not parse mget response"}

func (p *parser) findCRLF(index int) int {
	for i := index + 1; i < len(p.data); i++ {
		if p.data[i-1] == '\r' && p.data[i] == '\n' {
			return i + 1
		}
	}
	return -1
}

func (p *parser) prefixEqual(a, b byte) bool {
	return p.data[0] == a && p.data[1] == b
}

func (p *parser) pairEqual(i int, a, b byte) bool {
	return p.data[i] == a && p.data[i+1] == b
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func findNumber(data []byte, index int) (uint32, int) {
	foundIndex := -1
	var firstChar byte
	for i := index; i < len(data); i++ {
		if isDigit(data[i]) {
			foundIndex = i
			firstChar = data[i]
			break
		}
	}

	num := uint32(firstChar - '0')

	var i int
	for i = foundIndex + 1; i < len(data); i++ {
		if !isDigit(data[i]) {
			break
		}
		num *= 10
		num += uint32(data[i] - '0')
	}

	return num, i
}

func (p *parser) returnIfCRLF(resp mgetResponse) (mgetResponse, error) {
	if p.findCRLF(2) < 0 {
		return mgetResponse{}, errInvalidMGet
	}
	return resp, nil
}

func (p *parser) readMGet() (mgetResponse, error) {
	if len(p.data) < 4 {
		return mgetResponse{}, errInvalidMGet
	}

	if p.prefixEqual('E', 'N') {
		return p.returnIfCRLF(mgetResponse{
			responseType: mgetResponseTypeEN,
		})
	}

	if p.prefixEqual('H', 'D') {
		return p.returnIfCRLF(mgetResponse{
			responseType: mgetResponseTypeHD,
		})
	}

	if p.prefixEqual('V', 'A') {
		num, index := findNumber(p.data, 3)
		crlfIndex := p.findCRLF(index)
		if crlfIndex < 0 {
			return mgetResponse{}, errInvalidMGet
		}

		dataEnd := crlfIndex + int(num)
		if dataEnd+2 > len(p.data) {
			return mgetResponse{}, errInvalidMGet
		}

		data := make([]byte, num)
		copy(data, p.data[crlfIndex:dataEnd])

		if !p.pairEqual(dataEnd, '\r', '\n') {
			return mgetResponse{}, errInvalidMGet
		}

		return mgetResponse{
			responseType: mgetResponseTypeVA,
			size:         num,
			data:         data,
		}, nil
	}

	return mgetResponse{}, errInvalidMGet
}
