package memcache

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"sync"
	"testing"
	"time"
)

func newCommandFromString(s string) *commandData {
	cmd := newCommand()
	cmd.cmdCount = 1
	cmd.data = []byte(s)
	return cmd
}

func newNetConnForTest(buf *bytes.Buffer) netConn {
	return netConn{
		writer: NoopFlusher(buf),
		reader: nil,
		closer: nil,
	}
}

func TestSender_Publish(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 8)

	_ = s.publish(newCommandFromString("mg key01 v\r\n"))
	_ = s.publish(newCommandFromString("mg key02 v k\r\n"))

	assert.Equal(t, "mg key01 v\r\nmg key02 v k\r\n", buf.String())

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 2, n)
	assert.Equal(t, "mg key01 v\r\n", string(cmdList[0].data))
	assert.Equal(t, "mg key02 v k\r\n", string(cmdList[1].data))
}

func TestSender_Publish_Concurrent(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 8)

	s.ncMut.Lock()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()

		_ = s.publish(newCommandFromString("mg key01\r\n"))
	}()

	go func() {
		defer wg.Done()

		_ = s.publish(newCommandFromString("mg key02 v\r\n"))
	}()

	go func() {
		defer wg.Done()

		_ = s.publish(newCommandFromString("mg key03 v\r\n"))
	}()

	time.Sleep(10 * time.Millisecond)

	s.ncMut.Unlock()
	wg.Wait()

	cmdList := make([]*commandData, 10)
	n := s.readSentCommands(cmdList)
	assert.Equal(t, 3, n)

	result := map[string]struct{}{
		string(cmdList[0].data): {},
		string(cmdList[1].data): {},
		string(cmdList[2].data): {},
	}
	assert.Equal(t, map[string]struct{}{
		"mg key01\r\n":   {},
		"mg key02 v\r\n": {},
		"mg key03 v\r\n": {},
	}, result)
}

//revive:disable:cognitive-complexity
func TestSender_Publish_Stress_Test(t *testing.T) {
	var buf bytes.Buffer
	s := newSender(newNetConnForTest(&buf), 2)
	assert.Equal(t, 4, s.sendBufMax)

	const numRounds = 200000

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()

		for i := 0; i < numRounds; i++ {
			key := fmt.Sprintf("A:KEY:%09d", i)
			_ = s.publish(newCommandFromString(fmt.Sprintf("mg %s\r\n", key)))
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numRounds; i++ {
			key := fmt.Sprintf("B:KEY:%09d", i)
			_ = s.publish(newCommandFromString(fmt.Sprintf("mg %s\r\n", key)))
		}
	}()

	go func() {
		defer wg.Done()

		cmdList := make([]*commandData, 29)
		total := 0
		aKeys := 0
		bKeys := 0
		for total < 2*numRounds {
			n := s.readSentCommands(cmdList)
			for _, cmd := range cmdList[:n] {
				key := strings.Fields(string(cmd.data))[1]
				strList := strings.Split(key, ":")
				prefix := strList[0]
				num := strList[2]

				if prefix == "A" {
					if num != fmt.Sprintf("%09d", aKeys) {
						panic("Missing key")
					}
					aKeys++
				} else if prefix == "B" {
					if num != fmt.Sprintf("%09d", bKeys) {
						panic("Missing key")
					}
					bKeys++
				} else {
					panic("Invalid prefix")
				}
			}
			total += n
		}
		fmt.Println(aKeys, bKeys)
	}()

	wg.Wait()
}

//revive:enable:cognitive-complexity
