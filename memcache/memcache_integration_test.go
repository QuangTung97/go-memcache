package memcache

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/QuangTung97/go-memcache/memcache/netconn"
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

	fn1 := pipe.MGet("KEY01", MGetOptions{})
	fn2 := pipe.MDel("KEY02", MDelOptions{})
	fn3 := pipe.MSet("KEY03", []byte("some value"), MSetOptions{})

	resp, err := fn1()
	delResp, delErr := fn2()
	setResp, setErr := fn3()

	mut.Lock()
	assert.Equal(t, logErr, err)
	assert.Equal(t, logErr, delErr)
	assert.Equal(t, logErr, setErr)
	mut.Unlock()

	assert.Equal(t, MGetResponse{}, resp)
	assert.Equal(t, MDelResponse{}, delResp)
	assert.Equal(t, MSetResponse{}, setResp)
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

	writeErr  error
	startChan chan struct{}
}

func (c *connRecorder) Write(b []byte) (n int, err error) {
	if c.startChan != nil {
		<-c.startChan
	}

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

	fn1 := p1.MSet("key01", []byte("some value 01"), MSetOptions{})
	fn2 := p2.MSet("key02", []byte("some value 02"), MSetOptions{})

	p1.Execute()
	p2.Execute()

	resp, err := fn1()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, resp)

	resp, err = fn2()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, resp)

	assert.Equal(t, "ms key01 13\r\nsome value 01\r\n", string(recorder1.data))
	assert.Equal(t, "ms key02 13\r\nsome value 02\r\n", string(recorder2.data))
}

//revive:disable-next-line:cognitive-complexity
func TestClient__ReadTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", ":10099")
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}

			go func() {
				var data [1024]byte
				for {
					n, err := conn.Read(data[:])
					if err == io.EOF {
						fmt.Println("Connection EOF")
						return
					}
					fmt.Println("Conn Data:", string(data[:n]))
				}
			}()
		}
	}()

	c, err := New("localhost:10099", 1,
		WithNetConnOptions(netconn.WithReadTimeout(200*time.Millisecond)),
	)
	if err != nil {
		panic(err)
	}

	pipe := c.Pipeline()
	defer pipe.Finish()

	start := time.Now()
	resp, err := pipe.MGet("KEY01", MGetOptions{})()
	getDuration := time.Since(start)

	assert.Greater(t, getDuration, 150*time.Millisecond)
	assert.Less(t, getDuration, 250*time.Millisecond)

	assert.Equal(t, true, errors.Is(err, os.ErrDeadlineExceeded))
	assert.Equal(t, MGetResponse{}, resp)

	fmt.Println(resp, err, getDuration)

	_ = lis.Close()
	wg.Wait()
}

func TestClient__WriteTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", ":10099")
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	var connMut sync.Mutex
	var connList []net.Conn

	go func() {
		defer wg.Done()

		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}

			connMut.Lock()
			connList = append(connList, conn)
			connMut.Unlock()
		}
	}()

	c, err := New("localhost:10099", 1,
		WithNetConnOptions(netconn.WithWriteTimeout(200*time.Millisecond)),
	)
	if err != nil {
		panic(err)
	}

	pipe := c.Pipeline()
	defer pipe.Finish()

	start := time.Now()

	fnList := make([]func() (MSetResponse, error), 0)
	for i := 0; i < 10; i++ {
		fn := pipe.MSet("KEY01", []byte(strings.Repeat("A", 1<<19)), MSetOptions{})
		fnList = append(fnList, fn)
	}

	for _, fn := range fnList {
		_, err = fn()
	}

	duration := time.Since(start)

	assert.Greater(t, duration, 150*time.Millisecond)
	assert.Less(t, duration, 500*time.Millisecond)

	assert.Equal(t, true, errors.Is(err, os.ErrDeadlineExceeded))

	fmt.Println(err, duration)

	_ = lis.Close()
	wg.Wait()

	connMut.Lock()
	for _, conn := range connList {
		_ = conn.Close()
	}
	connMut.Unlock()
}

func TestClient_Connect_To_Memcached_Need_Password__But_Not_Provided(t *testing.T) {
	c, err := New("localhost:11212", 1)
	assert.Equal(t, nil, err)
	t.Cleanup(func() { _ = c.Close() })

	pipe := c.Pipeline()
	t.Cleanup(pipe.Finish)

	const key = "key01"
	data := []byte("some data")

	_, err = pipe.MSet(key, data, MSetOptions{})()
	assert.Equal(t, ErrClientError{Message: "unauthenticated"}, err)

	resp, err := pipe.MGet(key, MGetOptions{})()
	assert.Equal(t, ErrClientError{Message: "unauthenticated"}, err)
	assert.Equal(t, MGetResponse{}, resp)
}

func TestClient_Connect_To_Memcached_Need_Password_And_Provided(t *testing.T) {
	plain, err := netconn.NewPasswordAuth("user01", "password01")
	assert.Equal(t, nil, err)

	c, err := New("localhost:11212", 1, WithDialFunc(plain.GetDialFunc(net.DialTimeout)))
	assert.Equal(t, nil, err)
	t.Cleanup(func() { _ = c.Close() })

	pipe := c.Pipeline()
	t.Cleanup(pipe.Finish)

	const key = "key01"
	data := []byte("some data")

	_, err = pipe.MSet(key, data, MSetOptions{})()
	assert.Equal(t, nil, err)

	resp, err := pipe.MGet(key, MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: data,
	}, resp)
}

func TestClient_Connect_To_Memcached_Need_Password_And_Wrong_Pass_Provided(t *testing.T) {
	plain, err := netconn.NewPasswordAuth("user", "pass")
	assert.Equal(t, nil, err)

	c, err := New("localhost:11212", 1, WithDialFunc(plain.GetDialFunc(net.DialTimeout)))
	assert.Equal(t, nil, err)
	t.Cleanup(func() { _ = c.Close() })

	pipe := c.Pipeline()
	t.Cleanup(pipe.Finish)

	const key = "key01"
	data := []byte("some data")

	_, err = pipe.MSet(key, data, MSetOptions{})()
	assert.Equal(t, netconn.ErrInvalidUsernamePassword, err)

	resp, err := pipe.MGet(key, MGetOptions{})()
	assert.Equal(t, netconn.ErrInvalidUsernamePassword, err)
	assert.Equal(t, MGetResponse{}, resp)
}

func TestClient_Connect_To_Memcached_Not_Need_Password(t *testing.T) {
	plain, err := netconn.NewPasswordAuth("user", "pass")
	assert.Equal(t, nil, err)

	c, err := New("localhost:11211", 1, WithDialFunc(plain.GetDialFunc(net.DialTimeout)))
	assert.Equal(t, nil, err)
	t.Cleanup(func() { _ = c.Close() })

	pipe := c.Pipeline()
	t.Cleanup(pipe.Finish)

	const key = "key01"
	data := []byte("some data")

	setResp, err := pipe.MSet(key, data, MSetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{Type: MSetResponseTypeHD}, setResp)

	resp, err := pipe.MGet(key, MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: data,
	}, resp)

	resp, err = pipe.MGet("memcached_auth", MGetOptions{})()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("user pass"),
	}, resp)
}

func newDialFuncWithRecorder() (netconn.DialFunc, *connRecorder, chan struct{}) {
	startCh := make(chan struct{}, 100)
	recorder := &connRecorder{
		startChan: startCh,
	}
	dialFunc := func(network, address string, timeout time.Duration) (net.Conn, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			panic(err)
		}
		recorder.Conn = conn
		return recorder, nil
	}
	return dialFunc, recorder, startCh
}

func TestClient__With_WriteLimit__And_Max_Command_Count(t *testing.T) {
	dialFunc, recorder, startChan := newDialFuncWithRecorder()

	c, err := New("localhost:11211", 1,
		WithDialFunc(dialFunc),
		WithWriteLimit(4),
		WithMaxCommandsPerBatch(2),
	)
	assert.Equal(t, nil, err)

	p := c.Pipeline()
	defer p.Finish()

	startChan <- struct{}{}
	pipelineFlushAll(p)

	fn01 := p.MSet("key01", []byte("some value 01"), MSetOptions{})
	fn02 := p.MGet("key01", MGetOptions{})

	fn03 := p.MSet("key02", []byte("data 02"), MSetOptions{})
	fn04 := p.MSet("key03", []byte("data 03"), MSetOptions{})

	fn05 := p.MSet("key04", []byte("data 04"), MSetOptions{})

	pipe2 := c.Pipeline()
	defer pipe2.Finish()

	fn06 := pipe2.MSet("key05", []byte("data value 05"), MSetOptions{})
	fn07 := pipe2.MGet("key05", MGetOptions{})

	p.Execute()
	time.Sleep(5 * time.Millisecond)
	pipe2.Execute()
	close(startChan)

	_, err = fn01()
	assert.Equal(t, nil, err)

	setResp, err := fn06()
	assert.Equal(t, nil, err)
	assert.Equal(t, MSetResponse{
		Type: MSetResponseTypeHD,
	}, setResp)

	resp, err := fn02()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("some value 01"),
	}, resp)

	_, _ = fn03()
	_, _ = fn04()
	_, _ = fn05()

	resp, err = fn07()
	assert.Equal(t, nil, err)
	assert.Equal(t, MGetResponse{
		Type: MGetResponseTypeVA,
		Data: []byte("data value 05"),
	}, resp)

	assert.Equal(t,
		"flush_all\r\n"+
			"ms key01 13\r\nsome value 01\r\nmg key01 v\r\n"+
			"ms key02 7\r\ndata 02\r\n"+
			"ms key03 7\r\ndata 03\r\n"+
			"ms key05 13\r\ndata value 05\r\n"+
			"mg key05 v\r\n"+
			"ms key04 7\r\ndata 04\r\n",
		string(recorder.data))

	_ = c.Close()

	send := c.conns[0].core.sender
	assert.Equal(t, uint64(8), send.selector.writeLimiter.cmdWriteCount)
	assert.Equal(t, uint64(8), send.selector.writeLimiter.cmdReadCount.Load())
}

func TestClient_Health_Check(t *testing.T) {
	dialFunc, recorder, startChan := newDialFuncWithRecorder()
	close(startChan)

	c, err := New(
		"localhost:11211", 1,
		WithHealthCheckDuration(200*time.Millisecond),
		WithDialFunc(dialFunc),
	)
	assert.Equal(t, nil, err)

	time.Sleep(300 * time.Millisecond)

	err = c.Close()
	assert.Equal(t, nil, err)

	assert.Equal(t, "version\r\n", string(recorder.data))
}

func TestClient_NewPipeline_Without_Exec_Anything__Conn_Seq_Should_Not_Change(t *testing.T) {
	c, err := New("localhost:11211", 2)
	assert.Equal(t, nil, err)

	t.Cleanup(func() { _ = c.Close() })

	pipe := c.Pipeline()
	pipe.Finish()

	assert.Equal(t, uint64(0), c.next.Load())
}
