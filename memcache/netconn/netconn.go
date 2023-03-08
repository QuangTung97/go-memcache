package netconn

import (
	"bufio"
	"io"
	"net"
	"time"
)

// FlushWriter ...
type FlushWriter interface {
	io.Writer
	Flush() error
}

// NetConn ...
type NetConn struct {
	Writer FlushWriter
	Closer io.Closer
	Reader io.ReadCloser
}

type config struct {
	dialFunc func(network, address string, timeout time.Duration) (net.Conn, error)

	tcpKeepAliveDuration time.Duration

	connectTimeout time.Duration
	writeTimeout   time.Duration
	readTimeout    time.Duration

	bufferSize int
}

func computeConfig(options ...Option) config {
	conf := config{
		dialFunc: net.DialTimeout,

		tcpKeepAliveDuration: 30 * time.Second,

		connectTimeout: 10 * time.Second,
		readTimeout:    1 * time.Second,
		writeTimeout:   1 * time.Second,

		bufferSize: 16 * 1024,
	}

	for _, fn := range options {
		fn(&conf)
	}

	return conf
}

// DialNewConn ...
func DialNewConn(addr string, options ...Option) (NetConn, error) {
	conf := computeConfig(options...)

	nc, err := conf.dialFunc("tcp", addr, conf.connectTimeout)
	if err != nil {
		return NetConn{}, err
	}

	tcpNetConn, ok := nc.(*net.TCPConn)
	if ok {
		if err := tcpNetConn.SetKeepAlive(true); err != nil {
			return NetConn{}, err
		}
		if err := tcpNetConn.SetKeepAlivePeriod(conf.tcpKeepAliveDuration); err != nil {
			return NetConn{}, err
		}
	}

	writer := bufio.NewWriterSize(nc, conf.bufferSize)
	return NetConn{
		Writer: writer,
		Closer: nc,
		Reader: nc,
	}, nil
}

type noopFlusher struct {
	io.Writer
}

func (f noopFlusher) Flush() error {
	return nil
}

// NoopFlusher ...
func NoopFlusher(w io.Writer) FlushWriter {
	return noopFlusher{Writer: w}
}

// Error Connection

type errorConn struct {
	err error
}

func (w *errorConn) Write([]byte) (n int, err error) {
	return 0, w.err
}

func (w *errorConn) Read([]byte) (n int, err error) {
	return 0, w.err
}

// ErrorNetConn ...
func ErrorNetConn(err error) NetConn {
	conn := &errorConn{
		err: err,
	}

	reader := io.NopCloser(conn)

	return NetConn{
		Writer: NoopFlusher(conn),
		Closer: reader,
		Reader: reader,
	}
}

// Option ...
type Option func(conf *config)

// WithBufferSize ...
func WithBufferSize(size int) Option {
	return func(conf *config) {
		conf.bufferSize = size
	}
}

// WithTCPKeepAliveDuration sets the tcp keep alive duration
func WithTCPKeepAliveDuration(d time.Duration) Option {
	return func(conf *config) {
		conf.tcpKeepAliveDuration = d
	}
}

// WithDialFunc ...
func WithDialFunc(dialFunc func(network, address string, timeout time.Duration) (net.Conn, error)) Option {
	return func(conf *config) {
		conf.dialFunc = dialFunc
	}
}
