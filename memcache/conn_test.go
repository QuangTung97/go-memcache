package memcache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func connFlushAll(c *clientConn) {
	cmd := newCommandFromString("flush_all\r\n")
	c.pushCommand(cmd)
	cmd.waitCompleted()
}

func TestConn_Simple_Get_Miss(t *testing.T) {
	c := newConn("localhost:11211", newPipelineCommandListPool())
	defer func() {
		c.shutdown()
		c.waitCloseCompleted()
	}()

	connFlushAll(c)

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.pushCommand(cmd1)
	cmd1.waitCompleted()

	assert.Equal(t, "EN\r\n", string(cmd1.responseData))
}

func TestConn_Get_Multi_Keys_All_Missed(t *testing.T) {
	c := newConn("localhost:11211", newPipelineCommandListPool())
	defer func() {
		c.shutdown()
		c.waitCloseCompleted()
	}()

	connFlushAll(c)

	cmd1 := newCommandFromString("mg key01 v\r\nmg key02 v\r\nmg key03 v\r\n")
	cmd1.cmdCount = 3
	c.pushCommand(cmd1)
	cmd1.waitCompleted()

	assert.Equal(t, "EN\r\nEN\r\nEN\r\n", string(cmd1.responseData))
}

func TestConn_Set_Get(t *testing.T) {
	c := newConn("localhost:11211", newPipelineCommandListPool())
	defer func() {
		c.shutdown()
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
		"VA 4\r\n",
		"OK\r\n",
	}

	assert.Equal(t, strings.Join(results, ""), string(cmd.responseData))
	assert.Equal(t, [][]byte{
		[]byte("ABCD"),
	}, cmd.responseBinaries)
}

func TestConn_Shutdown(t *testing.T) {
	c := newConn("localhost:11211", newPipelineCommandListPool())

	cmd1 := newCommandFromString("mg key01 v\r\n")
	c.pushCommand(cmd1)
	cmd1.waitCompleted()

	assert.Equal(t, "EN\r\n", string(cmd1.responseData))

	c.shutdown()
	c.waitCloseCompleted()
}
