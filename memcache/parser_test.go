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
				size:         5,
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
