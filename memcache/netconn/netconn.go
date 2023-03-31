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
	Reader io.ReadCloser // TODO only need io.Reader
}

type connTimeout interface {
	// SetReadDeadline sets the deadline for future Read calls
	// and any currently-blocked Read call.
	// A zero value for t means Read will not time out.
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline sets the deadline for future Write calls
	// and any currently-blocked Write call.
	// Even if write times out, it may return n > 0, indicating that
	// some data was successfully written.
	// A zero value for t means Write will not time out.
	SetWriteDeadline(t time.Time) error
}

type timeoutWriter struct {
	FlushWriter
	timeout      connTimeout
	writeTimeout time.Duration
}

func (w *timeoutWriter) Write(p []byte) (n int, err error) {
	err = w.timeout.SetWriteDeadline(time.Now().Add(w.writeTimeout))
	if err != nil {
		return 0, err
	}
	return w.FlushWriter.Write(p)
}

type timeoutReader struct {
	io.ReadCloser
	timeout     connTimeout
	readTimeout time.Duration
}

func (r *timeoutReader) Read(p []byte) (n int, err error) {
	err = r.timeout.SetReadDeadline(time.Now().Add(r.readTimeout))
	if err != nil {
		return 0, err
	}
	return r.ReadCloser.Read(p)
}

// DialFunc ...
type DialFunc = func(network, address string, timeout time.Duration) (net.Conn, error)

type config struct {
	dialFunc DialFunc

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

	var writer FlushWriter = bufio.NewWriterSize(nc, conf.bufferSize)

	writer = &timeoutWriter{
		FlushWriter:  writer,
		timeout:      nc,
		writeTimeout: conf.writeTimeout,
	}

	var reader io.ReadCloser = &timeoutReader{
		ReadCloser:  nc,
		timeout:     nc,
		readTimeout: conf.readTimeout,
	}

	return NetConn{
		Writer: writer,
		Closer: reader,
		Reader: reader,
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
func WithDialFunc(dialFunc DialFunc) Option {
	return func(conf *config) {
		conf.dialFunc = dialFunc
	}
}

// WithReadTimeout ...
func WithReadTimeout(d time.Duration) Option {
	return func(conf *config) {
		conf.readTimeout = d
	}
}

// WithWriteTimeout ...
func WithWriteTimeout(d time.Duration) Option {
	return func(conf *config) {
		conf.writeTimeout = d
	}
}
