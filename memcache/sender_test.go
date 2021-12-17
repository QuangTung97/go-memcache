package memcache

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSender_Publish(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(&buf)

	cmd1 := newCommand()
	cmd1.cmdCount = 1
	cmd1.data = []byte("mg key01 v\r\n")
	s.publish(cmd1)

	cmd2 := newCommand()
	cmd2.cmdCount = 1
	cmd2.data = []byte("mg key02 v k\r\n")
	s.publish(cmd2)

	assert.Equal(t, "mg key01 v\r\nmg key02 v k\r\n", buf.String())

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 2, n)
	assert.Equal(t, "mg key01 v\r\n", string(cmdList[0].data))
	assert.Equal(t, "mg key02 v k\r\n", string(cmdList[1].data))
}
