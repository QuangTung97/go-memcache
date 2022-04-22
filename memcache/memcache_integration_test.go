package memcache

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func resetGlobalNetDial() {
	globalNetDial = net.Dial
}

func TestClient_New_With_NumConns_Zero(t *testing.T) {
	c, err := New("localhost:11211", 0)
	assert.Equal(t, errors.New("numConns must > 0"), err)
	assert.Nil(t, c)
}

func TestClient_New_Connect_Error(t *testing.T) {
	globalNetDial = func(network, address string) (net.Conn, error) {
		return nil, errors.New("cannot connect to memcached")
	}
	defer resetGlobalNetDial()

	c, err := New("localhost:11211", 1)
	assert.Equal(t, errors.New("cannot connect to memcached"), err)
	assert.Nil(t, c)
}

func TestClient_Connection_Error_And_Retry(t *testing.T) {
	counter := uint64(0)
	var connection net.Conn
	globalNetDial = func(network, address string) (net.Conn, error) {
		atomic.AddUint64(&counter, 1)

		if counter > 1 && counter < 5 { // 2, 3, 4 => 30 millisecond
			return nil, errors.New("cannot connect to memcached")
		}

		conn, err := net.Dial(network, address)
		if err != nil {
			panic(err)
		}
		connection = conn

		return connection, nil
	}
	defer resetGlobalNetDial()

	c, err := New("localhost:11211", 1, WithRetryDuration(10*time.Millisecond))
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	resp, err := p.MGet("key01", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeEN,
	}, resp)

	// Close Connection
	err = connection.Close()
	assert.Equal(t, nil, err)

	resp, err = p.MGet("key01", MGetOptions{})()
	assert.NotNil(t, err)

	time.Sleep(15 * time.Millisecond)

	resp, err = p.MGet("key01", MGetOptions{})()
	assert.NotNil(t, err)

	time.Sleep(35 * time.Millisecond)

	resp, err = p.MGet("key01", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{Type: MGetResponseTypeEN}, resp)

	assert.Equal(t, uint64(5), counter)
}
