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
