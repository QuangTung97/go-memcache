package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConn_Simple_Get_Miss(t *testing.T) {
	c, err := newConn("localhost:11211")
	assert.Equal(t, nil, err)

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.pushCommand(cmd1)
	cmd1.wait()

	assert.Equal(t, "EN\r\n", string(cmd1.data))
}
