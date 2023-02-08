package stats

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type statsParser struct {
	lastErr error
	item    statItem
	scanner *bufio.Scanner
}

func newStatsParser(scanner *bufio.Scanner) *statsParser {
	return &statsParser{
		scanner: scanner,
	}
}

// Error ...
type Error struct {
	Msg string
}

func (e *Error) Error() string {
	return fmt.Sprint("stats: ", e.Msg)
}

// NewError ...
func NewError(msg string) *Error {
	return &Error{
		Msg: msg,
	}
}

func (p *statsParser) next() bool {
	ok := p.scanner.Scan()

	p.lastErr = p.scanner.Err()
	if p.lastErr != nil {
		return false
	}

	if !ok {
		p.lastErr = io.EOF
		return false
	}

	line := p.scanner.Text()

	fields := strings.Fields(line)
	if len(fields) == 0 {
		p.lastErr = NewError("empty line")
		return false
	}

	if fields[0] == "END" {
		p.item = statItem{}
		return false
	}

	if fields[0] != "STAT" {
		p.lastErr = NewError("line not begin with STAT")
		return false
	}

	if len(fields) < 3 {
		p.lastErr = NewError("missing stat fields")
		return false
	}

	p.item = statItem{
		key:   fields[1],
		value: fields[2],
	}

	return true
}

func (p *statsParser) getError() error {
	return p.lastErr
}

func (p *statsParser) getItem() statItem {
	return p.item
}
