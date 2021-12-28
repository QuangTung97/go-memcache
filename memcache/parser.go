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

type parserFlags uint64

const (
	flagW parserFlags = 1 << iota // won cache lease
	flagX                         // stale data
	flagZ                         // already has winning flag
)

type mgetResponse struct {
	responseType mgetResponseType
	data         []byte
	flags        parserFlags
	cas          uint64
}

var errInvalidMGet = ErrBrokenPipe{reason: "can not parse mget response"}

func (p *parser) findCRLF(index int) int {
	for i := index + 1; i < len(p.data); i++ {
		if p.isCRLF(i - 1) {
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

func (p *parser) isCRLF(index int) bool {
	return p.pairEqual(index, '\r', '\n')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func findNumber(data []byte, index int) (uint64, int) {
	foundIndex := -1
	var firstChar byte
	for i := index; i < len(data); i++ {
		if isDigit(data[i]) {
			foundIndex = i
			firstChar = data[i]
			break
		}
	}

	num := uint64(firstChar - '0')

	var i int
	for i = foundIndex + 1; i < len(data); i++ {
		if !isDigit(data[i]) {
			break
		}
		num *= 10
		num += uint64(data[i] - '0')
	}

	return num, i // next index right after number
}

func (p *parser) returnIfCRLF(index int, resp mgetResponse) (mgetResponse, error) {
	if p.findCRLF(index) < 0 {
		return mgetResponse{}, errInvalidMGet
	}
	return resp, nil
}

func (p *parser) parseFlags(index int, resp *mgetResponse) (int, error) {
	flags := parserFlags(0)
	for i := index; i < len(p.data)-1; i++ {
		if p.data[i] == 'W' {
			flags |= flagW
			continue
		}
		if p.data[i] == 'X' {
			flags |= flagX
			continue
		}
		if p.data[i] == 'Z' {
			flags |= flagZ
			continue
		}
		if p.data[i] == 'c' {
			cas, nextIndex := findNumber(p.data, i+1)
			resp.cas = cas
			i = nextIndex - 1
			continue
		}

		if p.isCRLF(i) {
			resp.flags = flags
			return i + 2, nil
		}
	}
	return 0, errInvalidMGet
}

func (p *parser) readMGet() (mgetResponse, error) {
	if len(p.data) < 4 {
		return mgetResponse{}, errInvalidMGet
	}

	if p.prefixEqual('E', 'N') {
		return p.returnIfCRLF(2, mgetResponse{
			responseType: mgetResponseTypeEN,
		})
	}

	if p.prefixEqual('H', 'D') {
		resp := mgetResponse{
			responseType: mgetResponseTypeHD,
		}

		_, err := p.parseFlags(2, &resp)
		if err != nil {
			return mgetResponse{}, err
		}
		return resp, nil
	}

	if p.prefixEqual('V', 'A') {
		num, index := findNumber(p.data, 3)

		resp := mgetResponse{
			responseType: mgetResponseTypeVA,
		}

		crlfIndex, err := p.parseFlags(index, &resp)
		if err != nil {
			return mgetResponse{}, err
		}

		dataEnd := crlfIndex + int(num)
		if dataEnd+2 > len(p.data) {
			return mgetResponse{}, errInvalidMGet
		}

		data := make([]byte, num)
		copy(data, p.data[crlfIndex:dataEnd])
		resp.data = data

		if !p.pairEqual(dataEnd, '\r', '\n') {
			return mgetResponse{}, errInvalidMGet
		}

		return resp, nil
	}

	return mgetResponse{}, errInvalidMGet
}
