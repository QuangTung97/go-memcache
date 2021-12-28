package memcache

import "bytes"

type parser struct {
	data []byte
}

func initParser(p *parser, data []byte) {
	p.data = data
}

type mgetResponseType int

var serverErrorPrefix = []byte("SERVER_ERROR")

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

type msetResponseType int

const (
	msetResponseTypeHD msetResponseType = iota + 1 // STORED
	msetResponseTypeNS                             // NOT STORED
	msetResponseTypeEX                             // EXISTS, cas modified
	msetResponseTypeNF                             // NOT FOUND, cas not found
)

type msetResponse struct {
	responseType msetResponseType
}

type mdelResponseType int

const (
	mdelResponseTypeHD mdelResponseType = iota + 1 // DELETED
	mdelResponseTypeNF                             // NOT FOUND
	mdelResponseTypeEX                             // EXISTS, cas not match
)

type mdelResponse struct {
	responseType mdelResponseType
}

var errInvalidMGet = ErrBrokenPipe{reason: "can not parse mget response"}
var errInvalidMSet = ErrBrokenPipe{reason: "can not parse mset response"}
var errInvalidMDel = ErrBrokenPipe{reason: "can not parse mdel response"}

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

func (p *parser) parseMGetFlags(index int, resp *mgetResponse) (int, error) {
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

func (p *parser) readMGetHD() (mgetResponse, error) {
	resp := mgetResponse{
		responseType: mgetResponseTypeHD,
	}

	_, err := p.parseMGetFlags(2, &resp)
	if err != nil {
		return mgetResponse{}, err
	}
	return resp, nil
}

func (p *parser) readMGetVA() (mgetResponse, error) {
	num, index := findNumber(p.data, 3)

	resp := mgetResponse{
		responseType: mgetResponseTypeVA,
	}

	crlfIndex, err := p.parseMGetFlags(index, &resp)
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

func (p *parser) readServerError() error {
	index := len(serverErrorPrefix)
	crlfIndex := p.findCRLF(index + 1)
	if crlfIndex < 0 {
		return errInvalidMGet
	}
	data := make([]byte, crlfIndex-2-index-1)
	copy(data, p.data[index+1:])
	return NewServerError(string(data))
}

func (p *parser) isServerErrorPrefix() bool {
	return p.prefixEqual('S', 'E') && len(p.data) > len(serverErrorPrefix) &&
		bytes.Equal(p.data[:len(serverErrorPrefix)], serverErrorPrefix)
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
		return p.readMGetHD()
	}

	if p.prefixEqual('V', 'A') {
		return p.readMGetVA()
	}

	if p.isServerErrorPrefix() {
		return mgetResponse{}, p.readServerError()
	}

	return mgetResponse{}, errInvalidMGet
}

// Meta Set

func (p *parser) readMSetWithCRLF(respType msetResponseType) (msetResponse, error) {
	index := p.findCRLF(2)
	if index < 0 {
		return msetResponse{}, errInvalidMSet
	}
	return msetResponse{
		responseType: respType,
	}, nil
}

func (p *parser) readMSet() (msetResponse, error) {
	if len(p.data) < 4 {
		return msetResponse{}, errInvalidMSet
	}

	if p.prefixEqual('H', 'D') {
		return p.readMSetWithCRLF(msetResponseTypeHD)
	}
	if p.prefixEqual('N', 'S') {
		return p.readMSetWithCRLF(msetResponseTypeNS)
	}
	if p.prefixEqual('E', 'X') {
		return p.readMSetWithCRLF(msetResponseTypeEX)
	}
	if p.prefixEqual('N', 'F') {
		return p.readMSetWithCRLF(msetResponseTypeNF)
	}

	return msetResponse{}, errInvalidMSet
}

// Meta Delete

func (p *parser) readMDelWithCRLF(respType mdelResponseType) (mdelResponse, error) {
	index := p.findCRLF(2)
	if index < 0 {
		return mdelResponse{}, errInvalidMDel
	}
	return mdelResponse{
		responseType: respType,
	}, nil
}

func (p *parser) readMDel() (mdelResponse, error) {
	if len(p.data) < 4 {
		return mdelResponse{}, errInvalidMDel
	}
	if p.prefixEqual('H', 'D') {
		return p.readMDelWithCRLF(mdelResponseTypeHD)
	}
	if p.prefixEqual('N', 'F') {
		return p.readMDelWithCRLF(mdelResponseTypeNF)
	}
	if p.prefixEqual('E', 'X') {
		return p.readMDelWithCRLF(mdelResponseTypeEX)
	}
	return mdelResponse{}, errInvalidMDel
}
