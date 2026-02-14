package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Prefix string

const (
	ArrayPrefix        Prefix = "*"
	BulkStringPrefix   Prefix = "$"
	SimpleStringPrefix Prefix = "+"
	ErrorPrefix        Prefix = "-"
	IntegerPrefix      Prefix = ":"
)

func (p Prefix) String() string {
	return string(p)
}

func (p Prefix) IsPrefix(line string) bool {
	return strings.HasPrefix(line, p.String())
}

func EncodeSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

func EncodeError(s string) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", s))
}

func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func HandleEcho(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'echo' command")
	}
	return EncodeBulkString(args[0]), nil
}

// ReadArray reads a RESP array where all elements are bulk strings.
// It returns the array items as UTF-8 strings (without trailing CRLF).
func ReadArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !ArrayPrefix.IsPrefix(line) {
		return nil, fmt.Errorf("expected array prefix, got: %q", strings.TrimSpace(line))
	}
	countStr := strings.TrimSpace(strings.TrimPrefix(line, ArrayPrefix.String()))
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("invalid array length %q: %w", countStr, err)
	}

	items := make([]string, 0, count)
	for i := 0; i < count; i++ {
		item, err := readBulkString(r)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !BulkStringPrefix.IsPrefix(line) {
		return "", fmt.Errorf("expected bulk string prefix, got: %q", strings.TrimSpace(line))
	}

	lenStr := strings.TrimSpace(strings.TrimPrefix(line, BulkStringPrefix.String()))
	n, err := strconv.Atoi(lenStr)
	if err != nil {
		return "", fmt.Errorf("invalid bulk length %q: %w", lenStr, err)
	}
	if n < 0 {
		// Null bulk string
		return "", nil
	}

	buf := make([]byte, n+2) // include trailing CRLF
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return "", fmt.Errorf("bulk string missing CRLF")
	}
	return string(buf[:n]), nil
}
