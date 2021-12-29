package memcache

import "bytes"

type parser struct {
	data []byte
}

func initParser(p *parser, data []byte) {
	p.data = data
}

// MGetResponseType ...
type MGetResponseType int

var serverErrorPrefix = []byte("SERVER_ERROR")

const (
	// MGetResponseTypeVA ...
	MGetResponseTypeVA MGetResponseType = iota + 1
	// MGetResponseTypeHD ...
	MGetResponseTypeHD
	// MGetResponseTypeEN ...
	MGetResponseTypeEN
)

// MGetFlags ...
type MGetFlags uint64

const (
	// MGetFlagW ...
	MGetFlagW MGetFlags = 1 << iota // won cache lease
	// MGetFlagX ...
	MGetFlagX // stale data
	// MGetFlagZ ...
	MGetFlagZ // already has winning flag
)

// MGetResponse ...
type MGetResponse struct {
	Type  MGetResponseType
	Data  []byte
	Flags MGetFlags
	CAS   uint64
}

// MSetResponseType ...
type MSetResponseType int

const (
	// MSetResponseTypeHD ...
	MSetResponseTypeHD MSetResponseType = iota + 1 // STORED
	// MSetResponseTypeNS ...
	MSetResponseTypeNS // NOT STORED
	// MSetResponseTypeEX ...
	MSetResponseTypeEX // EXISTS, cas modified
	// MSetResponseTypeNF ...
	MSetResponseTypeNF // NOT FOUND, cas not found
)

// MSetResponse ...
type MSetResponse struct {
	Type MSetResponseType
}

// MDelResponseType ...
type MDelResponseType int

const (
	// MDelResponseTypeHD ...
	MDelResponseTypeHD MDelResponseType = iota + 1 // DELETED
	// MDelResponseTypeNF ...
	MDelResponseTypeNF // NOT FOUND
	// MDelResponseTypeEX ...
	MDelResponseTypeEX // EXISTS, cas not match
)

// MDelResponse ...
type MDelResponse struct {
	Type MDelResponseType
}

// ErrInvalidMGet ...
var ErrInvalidMGet = ErrBrokenPipe{reason: "can not parse mget response"}

// ErrInvalidMSet ...
var ErrInvalidMSet = ErrBrokenPipe{reason: "can not parse mset response"}

// ErrInvalidMDel ...
var ErrInvalidMDel = ErrBrokenPipe{reason: "can not parse mdel response"}

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

func (p *parser) returnIfCRLF(index int, resp MGetResponse) (MGetResponse, error) {
	nextIndex := p.findCRLF(index)
	if nextIndex < 0 {
		return MGetResponse{}, ErrInvalidMGet
	}
	p.skipData(nextIndex)
	return resp, nil
}

func (p *parser) parseMGetFlags(index int, resp *MGetResponse) (int, error) {
	flags := MGetFlags(0)
	for i := index; i < len(p.data)-1; i++ {
		if p.data[i] == 'W' {
			flags |= MGetFlagW
			continue
		}
		if p.data[i] == 'X' {
			flags |= MGetFlagX
			continue
		}
		if p.data[i] == 'Z' {
			flags |= MGetFlagZ
			continue
		}
		if p.data[i] == 'c' {
			cas, nextIndex := findNumber(p.data, i+1)
			resp.CAS = cas
			i = nextIndex - 1
			continue
		}

		if p.isCRLF(i) {
			resp.Flags = flags
			return i + 2, nil
		}
	}
	return 0, ErrInvalidMGet
}

func (p *parser) skipData(nextIndex int) {
	p.data = p.data[nextIndex:]
}

func (p *parser) readMGetHD() (MGetResponse, error) {
	resp := MGetResponse{
		Type: MGetResponseTypeHD,
	}

	nextIndex, err := p.parseMGetFlags(2, &resp)
	if err != nil {
		return MGetResponse{}, err
	}
	p.skipData(nextIndex)
	return resp, nil
}

func (p *parser) readMGetVA() (MGetResponse, error) {
	num, index := findNumber(p.data, 3)

	resp := MGetResponse{
		Type: MGetResponseTypeVA,
	}

	crlfIndex, err := p.parseMGetFlags(index, &resp)
	if err != nil {
		return MGetResponse{}, err
	}

	dataEnd := crlfIndex + int(num)
	if dataEnd+2 > len(p.data) {
		return MGetResponse{}, ErrInvalidMGet
	}

	data := make([]byte, num)
	copy(data, p.data[crlfIndex:dataEnd])
	resp.Data = data

	if !p.pairEqual(dataEnd, '\r', '\n') {
		return MGetResponse{}, ErrInvalidMGet
	}

	p.skipData(dataEnd + 2)
	return resp, nil
}

func (p *parser) readServerError() error {
	index := len(serverErrorPrefix)
	crlfIndex := p.findCRLF(index + 1)
	if crlfIndex < 0 {
		return ErrInvalidMGet
	}

	data := make([]byte, crlfIndex-2-index-1)
	copy(data, p.data[index+1:])

	p.skipData(crlfIndex)

	return NewServerError(string(data))
}

func (p *parser) isServerErrorPrefix() bool {
	return p.prefixEqual('S', 'E') && len(p.data) > len(serverErrorPrefix) &&
		bytes.Equal(p.data[:len(serverErrorPrefix)], serverErrorPrefix)
}

func (p *parser) readMGet() (MGetResponse, error) {
	if len(p.data) < 4 {
		return MGetResponse{}, ErrInvalidMGet
	}

	if p.prefixEqual('E', 'N') {
		return p.returnIfCRLF(2, MGetResponse{
			Type: MGetResponseTypeEN,
		})
	}

	if p.prefixEqual('H', 'D') {
		return p.readMGetHD()
	}

	if p.prefixEqual('V', 'A') {
		return p.readMGetVA()
	}

	if p.isServerErrorPrefix() {
		return MGetResponse{}, p.readServerError()
	}

	return MGetResponse{}, ErrInvalidMGet
}

// Meta Set

func (p *parser) readMSetWithCRLF(respType MSetResponseType) (MSetResponse, error) {
	index := p.findCRLF(2)
	if index < 0 {
		return MSetResponse{}, ErrInvalidMSet
	}
	p.skipData(index)
	return MSetResponse{
		Type: respType,
	}, nil
}

func (p *parser) readMSet() (MSetResponse, error) {
	if len(p.data) < 4 {
		return MSetResponse{}, ErrInvalidMSet
	}

	if p.prefixEqual('H', 'D') {
		return p.readMSetWithCRLF(MSetResponseTypeHD)
	}
	if p.prefixEqual('N', 'S') {
		return p.readMSetWithCRLF(MSetResponseTypeNS)
	}
	if p.prefixEqual('E', 'X') {
		return p.readMSetWithCRLF(MSetResponseTypeEX)
	}
	if p.prefixEqual('N', 'F') {
		return p.readMSetWithCRLF(MSetResponseTypeNF)
	}

	return MSetResponse{}, ErrInvalidMSet
}

// Meta Delete

func (p *parser) readMDelWithCRLF(respType MDelResponseType) (MDelResponse, error) {
	index := p.findCRLF(2)
	if index < 0 {
		return MDelResponse{}, ErrInvalidMDel
	}
	p.skipData(index)
	return MDelResponse{
		Type: respType,
	}, nil
}

func (p *parser) readMDel() (MDelResponse, error) {
	if len(p.data) < 4 {
		return MDelResponse{}, ErrInvalidMDel
	}
	if p.prefixEqual('H', 'D') {
		return p.readMDelWithCRLF(MDelResponseTypeHD)
	}
	if p.prefixEqual('N', 'F') {
		return p.readMDelWithCRLF(MDelResponseTypeNF)
	}
	if p.prefixEqual('E', 'X') {
		return p.readMDelWithCRLF(MDelResponseTypeEX)
	}
	return MDelResponse{}, ErrInvalidMDel
}
