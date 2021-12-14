package memcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSendBuffer(t *testing.T) {
	b := newSendBuffer(10)

	cmd1 := &commandData{}
	cmd2 := &commandData{}
	cmd3 := &commandData{}
	cmd4 := &commandData{}
	cmd5 := &commandData{}

	b.push(cmd1)
	b.push(cmd2)
	b.push(cmd3)
	b.push(cmd4)
	b.push(cmd5)

	b.clearRemain(3)

	assert.Equal(t, []*commandData{
		cmd4, cmd5,
	}, b.buf)
	assert.Equal(t, 10, cap(b.buf))
}
