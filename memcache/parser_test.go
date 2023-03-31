package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newParserStr(s string, binaries ...string) *parser {
	responseBinaries := make([][]byte, 0, len(binaries))
	for _, e := range binaries {
		responseBinaries = append(responseBinaries, []byte(e))
	}

	p := &parser{}
	initParser(p, []byte(s), responseBinaries)
	return p
}

func TestParser_Read_MGet(t *testing.T) {
	table := []struct {
		name     string
		data     string
		binaries []string
		err      error
		resp     MGetResponse
	}{
		{
			name: "empty",
			data: "",
			err:  ErrBrokenPipe{reason: "can not parse mget response"},
		},
		{
			name: "EN",
			data: "EN\r\n",
			resp: MGetResponse{
				Type: MGetResponseTypeEN,
			},
		},
		{
			name: "HD",
			data: "HD\r\n",
			resp: MGetResponse{
				Type: MGetResponseTypeHD,
			},
		},
		{
			name: "missing-lf",
			data: "EN\r",
			err:  ErrInvalidMGet,
		},
		{
			name: "missing-lf",
			data: "EN \r",
			err:  ErrInvalidMGet,
		},
		{
			name: "missing-lf",
			data: "HD \r",
			err:  ErrInvalidMGet,
		},
		{
			name:     "VA",
			data:     "VA 5\r\n",
			binaries: []string{"AABBC"},
			resp: MGetResponse{
				Type: MGetResponseTypeVA,
				Data: []byte("AABBC"),
			},
		},
		{
			name:     "VA-missing-binaries",
			data:     "VA 5\r\n",
			binaries: nil,
			err:      ErrInvalidMGet,
		},
		{
			name: "VA-missing-va-lf",
			data: "VA 5\rABCDEFSA",
			err:  ErrInvalidMGet,
		},
		{
			name: "invalid-prefix",
			data: "OK 10\r\n",
			err:  ErrInvalidMGet,
		},
		{
			name: "HD-with-flag-W",
			data: "HD W\r\n",
			resp: MGetResponse{
				Type:  MGetResponseTypeHD,
				Flags: MGetFlagW,
			},
		},
		{
			name: "HD-with-flag-Z",
			data: "HD Z\r\n",
			resp: MGetResponse{
				Type:  MGetResponseTypeHD,
				Flags: MGetFlagZ,
			},
		},
		{
			name: "HD-with-flag-X",
			data: "HD X\r\n",
			resp: MGetResponse{
				Type:  MGetResponseTypeHD,
				Flags: MGetFlagX,
			},
		},
		{
			name: "HD-with-3-flags",
			data: "HD W X Z\r\n",
			resp: MGetResponse{
				Type:  MGetResponseTypeHD,
				Flags: MGetFlagW | MGetFlagX | MGetFlagZ,
			},
		},
		{
			name: "HD-with-cas",
			data: "HD c123\r\n",
			resp: MGetResponse{
				Type: MGetResponseTypeHD,
				CAS:  123,
			},
		},
		{
			name: "HD-with-cas-and-X-Z",
			data: "HD c123 X Z\r\n",
			resp: MGetResponse{
				Type:  MGetResponseTypeHD,
				Flags: MGetFlagX | MGetFlagZ,
				CAS:   123,
			},
		},
		{
			name:     "VA-with-X-Z",
			data:     "VA 3 c123 X Z\r\n",
			binaries: []string{"XXX"},
			resp: MGetResponse{
				Type:  MGetResponseTypeVA,
				Flags: MGetFlagX | MGetFlagZ,
				Data:  []byte("XXX"),
				CAS:   123,
			},
		},
		{
			name: "server-error-with-msg",
			data: "SERVER_ERROR some message\r\n",
			err:  NewServerError("some message"),
		},
		{
			name: "prefix-server",
			data: "SERVER_ERR",
			err:  ErrInvalidMGet,
		},
		{
			name: "prefix-server",
			data: "SERV",
			err:  ErrInvalidMGet,
		},
		{
			name: "server-error-missing-lf",
			data: "SERVER_ERROR msg01\r",
			err:  ErrInvalidResponse,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			p := newParserStr(e.data, e.binaries...)
			resp, err := p.readMGet()
			assert.Equal(t, e.err, err)
			assert.Equal(t, e.resp, resp)
		})
	}
}

func TestParser_Read_MSet(t *testing.T) {
	table := []struct {
		name string
		data string
		err  error
		resp MSetResponse
	}{
		{
			name: "empty",
			data: "",
			err:  ErrInvalidMSet,
		},
		{
			name: "HD",
			data: "HD\r\n",
			resp: MSetResponse{
				Type: MSetResponseTypeHD,
			},
		},
		{
			name: "NS",
			data: "NS\r\n",
			resp: MSetResponse{
				Type: MSetResponseTypeNS,
			},
		},
		{
			name: "EX",
			data: "EX\r\n",
			resp: MSetResponse{
				Type: MSetResponseTypeEX,
			},
		},
		{
			name: "NF",
			data: "NF\r\n",
			resp: MSetResponse{
				Type: MSetResponseTypeNF,
			},
		},
		{
			name: "HD-missing-lf",
			data: "HD  \r",
			err:  ErrInvalidMSet,
		},
		{
			name: "invalid-prefix",
			data: "OK  \r",
			err:  ErrInvalidMSet,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			p := newParserStr(e.data)
			resp, err := p.readMSet()
			assert.Equal(t, e.err, err)
			assert.Equal(t, e.resp, resp)
		})
	}
}

func TestParser_Read_MDel(t *testing.T) {
	table := []struct {
		name string
		data string
		err  error
		resp MDelResponse
	}{
		{
			name: "empty",
			data: "",
			err:  ErrInvalidMDel,
		},
		{
			name: "HD-missing-lf",
			data: "HD  \r",
			err:  ErrInvalidMDel,
		},
		{
			name: "HD",
			data: "HD\r\n",
			resp: MDelResponse{
				Type: MDelResponseTypeHD,
			},
		},
		{
			name: "NF",
			data: "NF\r\n",
			resp: MDelResponse{
				Type: MDelResponseTypeNF,
			},
		},
		{
			name: "EX",
			data: "EX\r\n",
			resp: MDelResponse{
				Type: MDelResponseTypeEX,
			},
		},
		{
			name: "invalid-prefix",
			data: "OK\r\n",
			err:  ErrInvalidMDel,
		},
	}
	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			p := newParserStr(e.data)
			resp, err := p.readMDel()
			assert.Equal(t, e.err, err)
			assert.Equal(t, e.resp, resp)
		})
	}
}

func TestParser_Multi_MGet_HD_First(t *testing.T) {
	p := newParserStr("HD\r\nEN\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeHD,
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeEN,
	}, resp)
}

func TestParser_Multi_MGet_EN_First(t *testing.T) {
	p := newParserStr("EN\r\nHD\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeEN,
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeHD,
	}, resp)
}

func TestParser_Multi_MGet_VA_First(t *testing.T) {
	p := newParserStr("VA 4 c55 W\r\nHD\r\n", "AAAA")

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte("AAAA"),
		Flags: MGetFlagW,
		CAS:   55,
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeHD,
	}, resp)
}

func TestParser_Multi_MGet_Server_Error_First(t *testing.T) {
	p := newParserStr("SERVER_ERROR some msg\r\nHD c55\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, NewServerError("some msg"), err)
	assert.Equal(t, MGetResponse{}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeHD,
		CAS:  55,
	}, resp)
}

func TestParser_Multi_MGet_2_VA(t *testing.T) {
	p := newParserStr("VA 3\r\nVA 4 W c33\r\n",
		"565",
		"XXXX",
	)

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("565"),
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type:  MGetResponseTypeVA,
		Data:  []byte("XXXX"),
		CAS:   33,
		Flags: MGetFlagW,
	}, resp)
}

func TestParser_Multi_MSet_HD_First(t *testing.T) {
	p := newParserStr("HD\r\nNS\r\n")

	resp, err := p.readMSet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeHD,
	}, resp)

	resp, err = p.readMSet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeNS,
	}, resp)
}

func TestParser_Multi_MDel_HD_First(t *testing.T) {
	p := newParserStr("HD\r\nNF\r\n")

	resp, err := p.readMDel()
	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{
		Type: MDelResponseTypeHD,
	}, resp)

	resp, err = p.readMDel()
	assert.Equal(t, nil, err)
	assert.Equal(t, MDelResponse{
		Type: MDelResponseTypeNF,
	}, resp)
}

func TestParser_Server_Error_MGet(t *testing.T) {
	p := newParserStr("SERVER_ERROR some error\r\n")
	resp, err := p.readMGet()
	assert.Equal(t, ErrServerError{Message: "some error"}, err)
	assert.Equal(t, MGetResponse{}, resp)
}

func TestParser_Client_Error_MGet(t *testing.T) {
	p := newParserStr("CLIENT_ERROR some error\r\n")
	resp, err := p.readMGet()
	assert.Equal(t, ErrClientError{Message: "some error"}, err)
	assert.Equal(t, MGetResponse{}, resp)
}

func TestParser_Server_Error_MSet(t *testing.T) {
	p := newParserStr("SERVER_ERROR some error\r\n")
	resp, err := p.readMSet()
	assert.Equal(t, ErrServerError{Message: "some error"}, err)
	assert.Equal(t, MSetResponse{}, resp)
}

func TestParser_Server_Error_MDel(t *testing.T) {
	p := newParserStr("SERVER_ERROR some error\r\n")
	resp, err := p.readMDel()
	assert.Equal(t, ErrServerError{Message: "some error"}, err)
	assert.Equal(t, MDelResponse{}, resp)
}

func TestParser_FlushAll(t *testing.T) {
	p := newParserStr("OK\r\nHD\r\n")
	err := p.readFlushAll()
	assert.Equal(t, nil, err)

	resp, err := p.readMSet()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeHD,
	}, resp)
}

func TestParser_FlushAll_Missing_LF(t *testing.T) {
	p := newParserStr("OK\r")
	err := p.readFlushAll()
	assert.Equal(t, ErrInvalidResponse, err)
}

func TestParser_FlushAll_Not_OK(t *testing.T) {
	p := newParserStr("HD\r")
	err := p.readFlushAll()
	assert.Equal(t, ErrInvalidResponse, err)
}

func TestParser_FlushAll_Server_Error(t *testing.T) {
	p := newParserStr("SERVER_ERROR some error\r\n")
	err := p.readFlushAll()
	assert.Equal(t, NewServerError("some error"), err)
}
