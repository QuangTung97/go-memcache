package memcache

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func connFlushAll(c *clientConn) {
	cmd := newCommandFromString("flush_all\r\n")
	c.pushCommand(cmd)
	cmd.waitCompleted()
}

func TestConn_Simple_Get_Miss(t *testing.T) {
	c, err := newConn("localhost:11211")
	assert.Equal(t, nil, err)
	defer func() {
		_ = c.shutdown()
		c.waitCloseCompleted()
	}()

	connFlushAll(c)

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.pushCommand(cmd1)
	cmd1.waitCompleted()

	assert.Equal(t, "EN\r\n", string(cmd1.data))
}

func TestConn_Get_Multi_Keys_All_Missed(t *testing.T) {
	c, err := newConn("localhost:11211")
	assert.Equal(t, nil, err)
	defer func() {
		_ = c.shutdown()
		c.waitCloseCompleted()
	}()

	connFlushAll(c)

	cmd1 := newCommandFromString("mg key01 v\r\nmg key02 v\r\nmg key03 v\r\n")
	cmd1.cmdCount = 3
	c.pushCommand(cmd1)
	cmd1.waitCompleted()

	assert.Equal(t, "EN\r\nEN\r\nEN\r\n", string(cmd1.data))
}

func TestConn_Set_Get(t *testing.T) {
	c, err := newConn("localhost:11211")
	assert.Equal(t, nil, err)
	defer func() {
		_ = c.shutdown()
		c.waitCloseCompleted()
	}()

	commands := []string{
		"ms key01 4\r\nABCD\r\n",
		"mg key01 v\r\n",
		"flush_all\r\n",
	}

	cmd := newCommandFromString(strings.Join(commands, ""))
	cmd.cmdCount = 3
	c.pushCommand(cmd)
	cmd.waitCompleted()

	results := []string{
		"HD\r\n",
		"VA 4\r\nABCD\r\n",
		"OK\r\n",
	}

	assert.Equal(t, strings.Join(results, ""), string(cmd.data))
}

func TestConn_Shutdown(t *testing.T) {
	c, err := newConn("localhost:11211")
	assert.Equal(t, nil, err)

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.pushCommand(cmd1)
	cmd1.waitCompleted()

	assert.Equal(t, "EN\r\n", string(cmd1.data))

	err = c.shutdown()
	assert.Equal(t, nil, err)
	c.waitCloseCompleted()
}
