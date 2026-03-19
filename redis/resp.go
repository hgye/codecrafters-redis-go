package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
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

func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

func EncodeNullArray() []byte {
	return []byte("*-1\r\n")
}

func EncodeInteger(n int64) []byte {
	return []byte(fmt.Sprintf(":%d\r\n", n))
}

func HandleEcho(args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'echo' command")
	}
	return EncodeBulkString(args[0]), nil
}

func HandleSet(args []string, store *Store) ([]byte, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'set' command")
	}
	if len(args) != 2 && len(args) != 4 {
		return nil, errors.New("ERR syntax error")
	}

	key, value := args[0], args[1]
	if len(args) == 2 {
		store.Set(key, value)
		return EncodeSimpleString("OK"), nil
	}

	option := strings.ToUpper(args[2])
	ttlRaw := args[3]
	ttlN, err := strconv.ParseInt(ttlRaw, 10, 64)
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}
	if ttlN <= 0 {
		return nil, errors.New("ERR invalid expire time in 'set' command")
	}

	switch option {
	case "EX":
		store.SetWithExpiry(key, value, time.Duration(ttlN)*time.Second)
	case "PX":
		store.SetWithExpiry(key, value, time.Duration(ttlN)*time.Millisecond)
	default:
		return nil, errors.New("ERR syntax error")
	}
	return EncodeSimpleString("OK"), nil
}

func HandleGet(args []string, store *Store) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'get' command")
	}
	value, ok, isStream := store.Get(args[0])
	if isStream {
		return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if !ok {
		return EncodeNullBulkString(), nil
	}
	return EncodeBulkString(value), nil
}

func HandleType(args []string, store *Store) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'type' command")
	}
	return EncodeSimpleString(store.TypeOf(args[0])), nil
}

func HandleXAdd(args []string, store *Store) ([]byte, error) {
	if len(args) < 4 {
		return nil, errors.New("ERR wrong number of arguments for 'xadd' command")
	}
	if (len(args)-2)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'xadd' command")
	}

	key := args[0]
	id := args[1]
	fields := args[2:]

	entryID, err := store.AddStreamEntry(key, id, fields)
	if err != nil {
		return nil, err
	}

	return EncodeBulkString(entryID), nil
}

func HandleXRange(args []string, store *Store) ([]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'xrange' command")
	}

	entries, err := store.XRange(args[0], args[1], args[2])
	if err != nil {
		return nil, err
	}

	return EncodeStreamEntries(entries), nil
}

func HandleXRead(args []string, store *Store) ([]byte, error) {
	if len(args) < 3 {
		return nil, errors.New("ERR wrong number of arguments for 'xread' command")
	}

	count := 0
	blockMs := int64(-1)
	i := 0
	for i < len(args) {
		tok := strings.ToUpper(args[i])
		if tok == "STREAMS" {
			break
		}
		if i+1 >= len(args) {
			return nil, errors.New("ERR syntax error")
		}
		switch tok {
		case "COUNT":
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				return nil, errors.New("ERR value is not an integer or out of range")
			}
			count = n
			i += 2
		case "BLOCK":
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || n < 0 {
				return nil, errors.New("ERR value is not an integer or out of range")
			}
			blockMs = n
			i += 2
		default:
			return nil, errors.New("ERR syntax error")
		}
	}

	if i >= len(args) || strings.ToUpper(args[i]) != "STREAMS" {
		return nil, errors.New("ERR syntax error")
	}

	rest := args[i+1:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil, errors.New("ERR Unbalanced XREAD list of streams")
	}
	n := len(rest) / 2
	keys := rest[:n]
	rawIDs := rest[n:]

	startIDs, err := store.ResolveXReadStartIDs(keys, rawIDs)
	if err != nil {
		return nil, err
	}

	results, err := store.XRead(keys, startIDs, count)
	if err != nil {
		return nil, err
	}

	if len(results) > 0 {
		return EncodeXReadResults(results), nil
	}

	if blockMs < 0 {
		return EncodeNullArray(), nil
	}

	if blockMs == 0 {
		for {
			time.Sleep(10 * time.Millisecond)
			results, err = store.XRead(keys, startIDs, count)
			if err != nil {
				return nil, err
			}
			if len(results) > 0 {
				return EncodeXReadResults(results), nil
			}
		}
	}

	deadline := time.Now().Add(time.Duration(blockMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		results, err = store.XRead(keys, startIDs, count)
		if err != nil {
			return nil, err
		}
		if len(results) > 0 {
			return EncodeXReadResults(results), nil
		}
	}

	return EncodeNullArray(), nil
}

func HandleZAdd(args []string, store *Store) ([]byte, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, errors.New("ERR wrong number of arguments for 'zadd' command")
	}

	key := args[0]
	rawEntries := args[1:]
	entries := make([]ZSetEntry, 0, len(rawEntries)/2)
	for i := 0; i < len(rawEntries); i += 2 {
		score, err := strconv.ParseFloat(rawEntries[i], 64)
		if err != nil {
			return nil, errors.New("ERR value is not a valid float")
		}
		entries = append(entries, ZSetEntry{Member: rawEntries[i+1], Score: score})
	}

	added, err := store.ZAdd(key, entries)
	if err != nil {
		return nil, err
	}

	return EncodeInteger(added), nil
}

func HandleIncr(args []string, store *Store) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'incr' command")
	}

	v, err := store.Incr(args[0])
	if err != nil {
		return nil, err
	}

	return EncodeInteger(v), nil
}

func HandleRPush(args []string, store *Store) ([]byte, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'rpush' command")
	}

	length, err := store.RPush(args[0], args[1:])
	if err != nil {
		return nil, err
	}

	return EncodeInteger(length), nil
}

func HandleLPush(args []string, store *Store) ([]byte, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'lpush' command")
	}

	length, err := store.LPush(args[0], args[1:])
	if err != nil {
		return nil, err
	}

	return EncodeInteger(length), nil
}

func HandleLRange(args []string, store *Store) ([]byte, error) {
	if len(args) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'lrange' command")
	}

	start, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}

	items, err := store.LRange(args[0], start, stop)
	if err != nil {
		return nil, err
	}

	return EncodeArray(items), nil
}

func HandleLLen(args []string, store *Store) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'llen' command")
	}

	length, err := store.LLen(args[0])
	if err != nil {
		return nil, err
	}

	return EncodeInteger(length), nil
}

func HandleLPop(args []string, store *Store) ([]byte, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'lpop' command")
	}

	if len(args) == 1 {
		item, ok, err := store.LPop(args[0])
		if err != nil {
			return nil, err
		}
		if !ok {
			return EncodeNullBulkString(), nil
		}

		return EncodeBulkString(item), nil
	}

	count, err := strconv.Atoi(args[1])
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}
	if count <= 0 {
		return nil, errors.New("ERR value is out of range, must be positive")
	}

	popped, err := store.LPopCount(args[0], count)
	if err != nil {
		return nil, err
	}
	if len(popped) == 0 {
		return EncodeNullArray(), nil
	}

	return EncodeArray(popped), nil
}

func HandleBLPop(args []string, store *Store) ([]byte, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'blpop' command")
	}

	timeoutSeconds, err := strconv.ParseFloat(args[len(args)-1], 64)
	if err != nil {
		return nil, errors.New("ERR timeout is not a float or out of range")
	}
	if timeoutSeconds < 0 {
		return nil, errors.New("ERR timeout is negative")
	}

	keys := args[:len(args)-1]

	tryPop := func() ([]byte, error) {
		for _, key := range keys {
			item, ok, err := store.LPop(key)
			if err != nil {
				return nil, err
			}
			if ok {
				return EncodeArray([]string{key, item}), nil
			}
		}
		return nil, nil
	}

	if resp, err := tryPop(); err != nil {
		return nil, err
	} else if resp != nil {
		return resp, nil
	}

	if timeoutSeconds == 0 {
		for {
			time.Sleep(10 * time.Millisecond)
			if resp, err := tryPop(); err != nil {
				return nil, err
			} else if resp != nil {
				return resp, nil
			}
		}
	}

	deadline := time.Now().Add(time.Duration(timeoutSeconds * float64(time.Second)))
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		if resp, err := tryPop(); err != nil {
			return nil, err
		} else if resp != nil {
			return resp, nil
		}
	}

	return EncodeNullArray(), nil
}

func EncodeStreamEntries(entries []StreamEntry) []byte {
	var buf []byte
	buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(entries)))...)
	for _, entry := range entries {
		buf = append(buf, []byte("*2\r\n")...)
		buf = append(buf, EncodeBulkString(entry.ID)...)
		buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(entry.Fields)))...)
		for _, field := range entry.Fields {
			buf = append(buf, EncodeBulkString(field)...)
		}
	}
	return buf
}

func EncodeXReadResults(results []XReadStreamResult) []byte {
	var buf []byte
	buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(results)))...)
	for _, stream := range results {
		buf = append(buf, []byte("*2\r\n")...)
		buf = append(buf, EncodeBulkString(stream.Key)...)
		buf = append(buf, EncodeStreamEntries(stream.Entries)...)
	}
	return buf
}

func EncodeArray(items []string) []byte {
	var buf []byte
	buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(items)))...)
	for _, item := range items {
		buf = append(buf, EncodeBulkString(item)...)
	}
	return buf
}

func EncodeRESPArray(items [][]byte) []byte {
	var buf []byte
	buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(items)))...)
	for _, item := range items {
		buf = append(buf, item...)
	}
	return buf
}

func HandleKeys(args []string, store *Store) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'keys' command")
	}
	if args[0] != "*" {
		return nil, errors.New("ERR only KEYS * is supported")
	}
	keys := store.Keys()
	return EncodeArray(keys), nil
}

func HandleInfo(args []string, replica *ReplicaInfo) ([]byte, error) {
	if len(args) < 1 {
		return nil, errors.New("ERR wrong number of arguments for 'info' command")
	}
	section := strings.ToLower(args[0])
	switch section {
	case "replication":
		role := "master"
		replID := "8371445fff36d3332a088d7be77bf1419d907b2d"
		replOffset := int64(0)
		if replica != nil {
			role = "slave"
			replID = replica.ReplicaID
			replOffset = replica.Offset
		}
		info := fmt.Sprintf("# Replication\r\nrole:%s\r\nconnected_slaves:0\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0", role, replID, replOffset)
		return EncodeBulkString(info), nil
	default:
		return EncodeBulkString(""), nil
	}
}

func HandleConfig(args []string, cfg Config) ([]byte, error) {
	if len(args) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'config' command")
	}
	subcommand := strings.ToUpper(args[0])
	if subcommand != "GET" {
		return nil, errors.New("ERR unsupported CONFIG subcommand")
	}
	key := strings.ToLower(args[1])

	var value string
	switch key {
	case "dir":
		value = cfg.Dir
	case "dbfilename":
		value = cfg.DBFilename
	default:
		return EncodeArray([]string{}), nil
	}
	return EncodeArray([]string{key, value}), nil
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
