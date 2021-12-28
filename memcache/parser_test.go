package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newParserStr(s string) *parser {
	p := &parser{}
	initParser(p, []byte(s))
	return p
}

func TestParser_Read_MGet(t *testing.T) {
	table := []struct {
		name string
		data string
		err  error
		resp mgetResponse
	}{
		{
			name: "empty",
			data: "",
			err:  ErrBrokenPipe{reason: "can not parse mget response"},
		},
		{
			name: "EN",
			data: "EN\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeEN,
			},
		},
		{
			name: "HD",
			data: "HD\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
			},
		},
		{
			name: "missing-lf",
			data: "EN\r",
			err:  errInvalidMGet,
		},
		{
			name: "missing-lf",
			data: "EN \r",
			err:  errInvalidMGet,
		},
		{
			name: "missing-lf",
			data: "HD \r",
			err:  errInvalidMGet,
		},
		{
			name: "VA",
			data: "VA 5\r\nAABBC\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeVA,
				data:         []byte("AABBC"),
			},
		},
		{
			name: "VA-missing-data-lf",
			data: "VA 5\r\nAABBC\r",
			err:  errInvalidMGet,
		},
		{
			name: "VA-missing-wrong-lf",
			data: "VA 5\r\nAABBC\r3",
			err:  errInvalidMGet,
		},
		{
			name: "VA-missing-va-lf",
			data: "VA 5\rABCDEFSA",
			err:  errInvalidMGet,
		},
		{
			name: "invalid-prefix",
			data: "OK 10\r\n",
			err:  errInvalidMGet,
		},
		{
			name: "HD-with-flag-W",
			data: "HD W\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
				flags:        flagW,
			},
		},
		{
			name: "HD-with-flag-Z",
			data: "HD Z\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
				flags:        flagZ,
			},
		},
		{
			name: "HD-with-flag-X",
			data: "HD X\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
				flags:        flagX,
			},
		},
		{
			name: "HD-with-3-flags",
			data: "HD W X Z\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
				flags:        flagW | flagX | flagZ,
			},
		},
		{
			name: "HD-with-cas",
			data: "HD c123\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
				cas:          123,
			},
		},
		{
			name: "HD-with-cas-and-X-Z",
			data: "HD c123 X Z\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeHD,
				flags:        flagX | flagZ,
				cas:          123,
			},
		},
		{
			name: "VA-with-X-Z",
			data: "VA 3 c123 X Z\r\nXXX\r\n",
			resp: mgetResponse{
				responseType: mgetResponseTypeVA,
				flags:        flagX | flagZ,
				data:         []byte("XXX"),
				cas:          123,
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
			err:  errInvalidMGet,
		},
		{
			name: "prefix-server",
			data: "SERV",
			err:  errInvalidMGet,
		},
		{
			name: "server-error-missing-lf",
			data: "SERVER_ERROR msg01\r",
			err:  errInvalidMGet,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			p := newParserStr(e.data)
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
		resp msetResponse
	}{
		{
			name: "empty",
			data: "",
			err:  errInvalidMSet,
		},
		{
			name: "HD",
			data: "HD\r\n",
			resp: msetResponse{
				responseType: msetResponseTypeHD,
			},
		},
		{
			name: "NS",
			data: "NS\r\n",
			resp: msetResponse{
				responseType: msetResponseTypeNS,
			},
		},
		{
			name: "EX",
			data: "EX\r\n",
			resp: msetResponse{
				responseType: msetResponseTypeEX,
			},
		},
		{
			name: "NF",
			data: "NF\r\n",
			resp: msetResponse{
				responseType: msetResponseTypeNF,
			},
		},
		{
			name: "HD-missing-lf",
			data: "HD  \r",
			err:  errInvalidMSet,
		},
		{
			name: "invalid-prefix",
			data: "OK  \r",
			err:  errInvalidMSet,
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
		resp mdelResponse
	}{
		{
			name: "empty",
			data: "",
			err:  errInvalidMDel,
		},
		{
			name: "HD-missing-lf",
			data: "HD  \r",
			err:  errInvalidMDel,
		},
		{
			name: "HD",
			data: "HD\r\n",
			resp: mdelResponse{
				responseType: mdelResponseTypeHD,
			},
		},
		{
			name: "NF",
			data: "NF\r\n",
			resp: mdelResponse{
				responseType: mdelResponseTypeNF,
			},
		},
		{
			name: "EX",
			data: "EX\r\n",
			resp: mdelResponse{
				responseType: mdelResponseTypeEX,
			},
		},
		{
			name: "invalid-prefix",
			data: "OK\r\n",
			err:  errInvalidMDel,
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
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeHD,
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeEN,
	}, resp)
}

func TestParser_Multi_MGet_EN_First(t *testing.T) {
	p := newParserStr("EN\r\nHD\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeEN,
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeHD,
	}, resp)
}

func TestParser_Multi_MGet_VA_First(t *testing.T) {
	p := newParserStr("VA 4 c55 W\r\nAAAA\r\nHD\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeVA,
		data:         []byte("AAAA"),
		flags:        flagW,
		cas:          55,
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeHD,
	}, resp)
}

func TestParser_Multi_MGet_Server_Error_First(t *testing.T) {
	p := newParserStr("SERVER_ERROR some msg\r\nHD c55\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, NewServerError("some msg"), err)
	assert.Equal(t, mgetResponse{}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeHD,
		cas:          55,
	}, resp)
}

func TestParser_Multi_MGet_2_VA(t *testing.T) {
	p := newParserStr("VA 3\r\n565\r\nVA 4 W c33\r\nXXXX\r\n")

	resp, err := p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeVA,
		data:         []byte("565"),
	}, resp)

	resp, err = p.readMGet()
	assert.Equal(t, nil, err)
	assert.Equal(t, mgetResponse{
		responseType: mgetResponseTypeVA,
		data:         []byte("XXXX"),
		cas:          33,
		flags:        flagW,
	}, resp)
}

func TestParser_Multi_MSet_HD_First(t *testing.T) {
	p := newParserStr("HD\r\nNS\r\n")

	resp, err := p.readMSet()
	assert.Equal(t, nil, err)
	assert.Equal(t, msetResponse{
		responseType: msetResponseTypeHD,
	}, resp)

	resp, err = p.readMSet()
	assert.Equal(t, nil, err)
	assert.Equal(t, msetResponse{
		responseType: msetResponseTypeNS,
	}, resp)
}

func TestParser_Multi_MDel_HD_First(t *testing.T) {
	p := newParserStr("HD\r\nNF\r\n")

	resp, err := p.readMDel()
	assert.Equal(t, nil, err)
	assert.Equal(t, mdelResponse{
		responseType: mdelResponseTypeHD,
	}, resp)

	resp, err = p.readMDel()
	assert.Equal(t, nil, err)
	assert.Equal(t, mdelResponse{
		responseType: mdelResponseTypeNF,
	}, resp)
}
