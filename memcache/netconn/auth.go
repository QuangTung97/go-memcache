package netconn

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// ErrInvalidUsernamePassword ...
var ErrInvalidUsernamePassword = errors.New("netconn: invalid username password")

// PasswordAuth is simple ASCII password authentication
type PasswordAuth struct {
	username string
	password string
}

// NewPasswordAuth ...
func NewPasswordAuth(username string, password string) (*PasswordAuth, error) {
	return &PasswordAuth{
		username: username,
		password: password,
	}, nil
}

func tryReadBytes(reader io.Reader, numBytes int) ([]byte, error) {
	data := make([]byte, numBytes)
	nextData := data
	for len(nextData) > 0 {
		n, err := reader.Read(nextData)
		if err != nil {
			return nil, err
		}
		nextData = nextData[n:]
	}
	return data, nil
}

const storedString = "STORED\r\n"

// GetDialFunc ...
func (a *PasswordAuth) GetDialFunc(dialFunc DialFunc) DialFunc {
	return func(network, address string, timeout time.Duration) (net.Conn, error) {
		conn, err := dialFunc(network, address, timeout)
		if err != nil {
			return nil, err
		}

		n := len(a.username) + len(a.password) + 1
		msg := fmt.Sprintf("set memcached_auth 0 0 %d\r\n%s %s\r\n", n, a.username, a.password)

		_, err = conn.Write([]byte(msg))
		if err != nil {
			_ = conn.Close()
			return nil, err
		}

		data, err := tryReadBytes(conn, len(storedString))
		if err != nil {
			_ = conn.Close()
			return nil, err
		}

		if string(data) != storedString {
			_ = conn.Close()
			return nil, ErrInvalidUsernamePassword
		}

		return conn, nil
	}
}
