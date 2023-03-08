package memcache

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClient_New_With_NumConns_Zero(t *testing.T) {
	c, err := New("localhost:11211", 0)
	assert.Equal(t, errors.New("numConns must > 0"), err)
	assert.Nil(t, c)
}

func TestClient_New_Connect_Error(t *testing.T) {
	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		return nil, errors.New("cannot connect to memcached")
	}

	var mut sync.Mutex
	var logErr error

	c, err := New("localhost:11211", 1,
		WithDialFunc(dialFunc),
		WithDialErrorLogger(func(err error) {
			mut.Lock()
			defer mut.Unlock()
			logErr = err
		}),
	)
	assert.Equal(t, nil, err)
	assert.Equal(t, errors.New("cannot connect to memcached"), logErr)
	assert.NotNil(t, c)

	pipe := c.Pipeline()
	defer pipe.Finish()

	resp, err := pipe.MGet("KEY01", MGetOptions{})()

	mut.Lock()
	assert.Equal(t, logErr, err)
	mut.Unlock()

	assert.Equal(t, MGetResponse{}, resp)
}

func TestClient_Connection_Error_And_Retry(t *testing.T) {
	counter := uint64(0)
	var connection net.Conn
	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		atomic.AddUint64(&counter, 1)

		if counter > 1 && counter < 5 { // skip 2, and then 3, 4 => 30 millisecond total
			return nil, errors.New("cannot connect to memcached")
		}

		conn, err := net.Dial(network, address)
		if err != nil {
			panic(err)
		}
		connection = conn

		return connection, nil
	}

	c, err := New("localhost:11211", 1,
		WithDialFunc(dialFunc),
		WithRetryDuration(10*time.Millisecond),
		WithTCPKeepAliveDuration(30*time.Second),
	)
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

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

type connRecorder struct {
	data []byte
	net.Conn

	writeErr error
}

func (c *connRecorder) Write(b []byte) (n int, err error) {
	c.data = append(c.data, b...)
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return c.Conn.Write(b)
}

func TestClient_PushData_To_Connection_Correctly(t *testing.T) {
	var recorder *connRecorder
	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			panic(err)
		}
		recorder = &connRecorder{
			Conn: conn,
		}
		return recorder, nil
	}

	c, err := New("localhost:11211", 1, WithDialFunc(dialFunc), WithBufferSize(64*1024))
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	pipelineFlushAll(p)

	fn01 := p.MSet("key01", []byte("some value 01"), MSetOptions{})
	fn02 := p.MGet("key01", MGetOptions{})

	_, err = fn01()
	assert.Equal(t, nil, err)

	resp, err := fn02()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("some value 01"),
	}, resp)

	assert.Equal(t, "flush_all\r\nms key01 13\r\nsome value 01\r\nmg key01 v\r\n", string(recorder.data))
}

func TestClient_Two_Clients__Concurrent_Execute(t *testing.T) {
	var recorder1 *connRecorder
	var recorder2 *connRecorder

	counter := 0
	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		counter++

		conn, err := net.Dial(network, address)
		if err != nil {
			panic(err)
		}

		recorder := &connRecorder{
			Conn: conn,
		}
		if counter == 1 {
			recorder1 = recorder
		} else {
			recorder2 = recorder
		}

		return recorder, nil
	}

	c1, err := New("localhost:11211", 1, WithDialFunc(dialFunc))
	assert.Equal(t, nil, err)
	defer func() { _ = c1.Close() }()

	c2, err := New("localhost:11211", 1, WithDialFunc(dialFunc))
	assert.Equal(t, nil, err)
	defer func() { _ = c2.Close() }()

	assert.Equal(t, 2, counter)

	p1 := c1.Pipeline()
	defer p1.Finish()

	p2 := c2.Pipeline()
	defer p2.Finish()

	p1.MSet("key01", []byte("some value 01"), MSetOptions{})
	p2.MSet("key02", []byte("some value 02"), MSetOptions{})

	p1.Execute()
	p2.Execute()

	assert.Equal(t, "ms key01 13\r\nsome value 01\r\n", string(recorder1.data))
	assert.Equal(t, "ms key02 13\r\nsome value 02\r\n", string(recorder2.data))
}

func TestClient_Retry_On_TCP_Conn_Close__Error_EOF(t *testing.T) {
	for i := 0; i < 1000; i++ {
		doTestClientRetryOnTCPConnCloseErrorEOF(t)
	}
}

func doTestClientRetryOnTCPConnCloseErrorEOF(t *testing.T) {
	var recorder *connRecorder
	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			panic(err)
		}
		recorder = &connRecorder{
			Conn: conn,
		}
		return recorder, nil
	}

	c, err := New("localhost:11211", 1, WithDialFunc(dialFunc), WithDialErrorLogger(func(err error) {
		fmt.Println("CONNECTION ERROR:", err)
	}))
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	err = p.FlushAll()()
	assert.Equal(t, nil, err)

	resp, err := p.MSet("key01", []byte("some value 01"), MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, resp)

	recorder.writeErr = io.EOF

	resp, err = p.MSet("key01", []byte("some value 02"), MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, resp)
}

func TestClient_Retry_On_TCP_Conn_Close__Try(t *testing.T) {
	t.Skip()

	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		return net.Dial(network, address)
	}
	c, err := New("localhost:11211", 1, WithDialFunc(dialFunc))
	assert.Equal(t, nil, err)
	defer func() { _ = c.Close() }()

	p := c.Pipeline()
	defer p.Finish()

	for i := 0; i < 100; i++ {
		now := time.Now()
		resp, err := p.MSet("key01", []byte("some value 01"), MSetOptions{})()
		fmt.Println("DURATION:", time.Since(now))
		if err != nil {
			fmt.Println(reflect.TypeOf(err))
			fmt.Println(err)
		}
		fmt.Println(resp)
		time.Sleep(1 * time.Second)
	}
}
