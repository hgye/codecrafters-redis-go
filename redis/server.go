package redis

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Dir        string
	DBFilename string
}

type ReplicaInfo struct {
	MasterHost string
	MasterPort string
	ReplicaID  string
	Offset     int64
}

type replicaEntry struct {
	conn      net.Conn
	reader    *bufio.Reader
	mu        sync.Mutex
	ackOffset int64
}

type queuedCommand struct {
	parts []string
}

type Server struct {
	l            net.Listener
	store        *Store
	config       Config
	replica      *ReplicaInfo
	port         string
	masterConn   net.Conn
	masterReader *bufio.Reader
	replicaMu    sync.Mutex
	replicas     []*replicaEntry
	replOffset   int64
	masterOffset int64 // bytes propagated to replicas
	pubsubMu     sync.Mutex
	channelSubs  map[string]map[net.Conn]struct{}
	connSubs     map[net.Conn]map[string]struct{}
}

func NewServer(l net.Listener, cfg Config, replica *ReplicaInfo, port string) *Server {
	s := &Server{
		l:           l,
		store:       NewStore(),
		config:      cfg,
		replica:     replica,
		port:        port,
		channelSubs: make(map[string]map[net.Conn]struct{}),
		connSubs:    make(map[net.Conn]map[string]struct{}),
	}
	if err := LoadRDB(cfg, s.store); err != nil {
		fmt.Println("Warning: failed to load RDB:", err)
	}
	return s
}

func (s *Server) Start() {
	if s.replica != nil {
		if err := s.Handshake(); err != nil {
			fmt.Println("Error during handshake:", err)
			os.Exit(1)
		}
		go s.listenToMaster()
	}
	fmt.Println("Starting server...")
	for {
		fmt.Println("Waiting for connection...")
		conn, err := s.l.Accept()
		fmt.Println("Accepted connection")
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) {
	reader := bufio.NewReader(conn)
	defer s.unsubscribeConnection(conn)
	authenticatedUser := aclInitialAuthenticatedUser()
	inMulti := false
	queued := make([]queuedCommand, 0)
	for {
		// Prefer RESP arrays (Codecrafters sends commands as RESP arrays)
		b, err := reader.Peek(1)
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			conn.Close()
			return
		}

		var parts []string
		if len(b) == 1 && b[0] == '*' {
			parts, err = ReadArray(reader)
			if err != nil {
				fmt.Println("Error parsing RESP array:", err.Error())
				conn.Write(EncodeError("ERR protocol error"))
				continue
			}
		} else {
			line, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				conn.Close()
				return
			}
			cmd := strings.TrimSpace(line)
			if cmd == "" {
				continue
			}
			parts = []string{cmd}
		}

		if len(parts) == 0 {
			conn.Write(EncodeError("ERR protocol error"))
			continue
		}
		command := strings.ToUpper(parts[0])

		if inMulti {
			switch command {
			case "MULTI":
				conn.Write(EncodeError("ERR MULTI calls can not be nested"))
			case "DISCARD":
				queued = queued[:0]
				inMulti = false
				conn.Write(EncodeSimpleString("OK"))
			case "EXEC":
				replies := make([][]byte, 0, len(queued))
				for _, qc := range queued {
					reply, propagate, closeConn := s.executeCommand(qc.parts, conn, reader, &authenticatedUser)
					if reply == nil {
						reply = EncodeError("ERR unknown command")
					}
					replies = append(replies, reply)
					if propagate {
						s.propagate(qc.parts)
					}
					if closeConn {
						conn.Write(EncodeRESPArray(replies))
						return
					}
				}
				queued = queued[:0]
				inMulti = false
				conn.Write(EncodeRESPArray(replies))
			default:
				queued = append(queued, queuedCommand{parts: append([]string(nil), parts...)})
				conn.Write(EncodeSimpleString("QUEUED"))
			}
			continue
		}

		switch command {
		case "MULTI":
			inMulti = true
			queued = queued[:0]
			conn.Write(EncodeSimpleString("OK"))
		case "EXEC":
			conn.Write(EncodeError("ERR EXEC without MULTI"))
		case "DISCARD":
			conn.Write(EncodeError("ERR DISCARD without MULTI"))
		default:
			reply, propagate, closeConn := s.executeCommand(parts, conn, reader, &authenticatedUser)
			if reply != nil {
				conn.Write(reply)
			}
			if propagate {
				s.propagate(parts)
			}
			if closeConn {
				return
			}
		}
	}
}

func (s *Server) executeCommand(parts []string, conn net.Conn, reader *bufio.Reader, authenticatedUser *string) (reply []byte, propagate bool, closeConn bool) {
	if len(parts) == 0 {
		return EncodeError("ERR protocol error"), false, false
	}

	command := strings.ToUpper(parts[0])
	if command != "AUTH" && authenticatedUser != nil && *authenticatedUser == "" {
		return EncodeError("NOAUTH Authentication required."), false, false
	}
	inSubscribeMode := s.isSubscribedConnection(conn)
	if inSubscribeMode && !isAllowedInSubscribeMode(command) {
		return EncodeError(fmt.Sprintf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET / PUBLISH are allowed in this context", strings.ToLower(command))), false, false
	}
	args := parts[1:]

	switch command {
	case "AUTH":
		if authenticatedUser == nil {
			return EncodeError("ERR internal auth state unavailable"), false, false
		}
		resp, err := HandleAuth(args, authenticatedUser)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "PING":
		resp, err := handlePing(args, inSubscribeMode)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ECHO":
		resp, err := HandleEcho(args)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "SET":
		resp, err := HandleSet(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "INCR":
		resp, err := HandleIncr(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "RPUSH":
		resp, err := HandleRPush(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "LPUSH":
		resp, err := HandleLPush(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "LRANGE":
		resp, err := HandleLRange(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "LLEN":
		resp, err := HandleLLen(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "LPOP":
		resp, err := HandleLPop(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "BLPOP":
		resp, err := HandleBLPop(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "SUBSCRIBE":
		resp, err := s.handleSubscribe(conn, args)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "UNSUBSCRIBE":
		resp := s.handleUnsubscribe(conn, args)
		return resp, false, false
	case "PUBLISH":
		if len(args) != 2 {
			return EncodeError("ERR wrong number of arguments for 'publish' command"), false, false
		}
		n := s.publish(args[0], args[1])
		return EncodeInteger(int64(n)), false, false
	case "ZADD":
		resp, err := HandleZAdd(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "GEOADD":
		resp, err := HandleGeoAdd(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "GEOPOS":
		resp, err := HandleGeoPos(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ZRANK":
		resp, err := HandleZRank(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ZRANGE":
		resp, err := HandleZRange(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ZCARD":
		resp, err := HandleZCard(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ZSCORE":
		resp, err := HandleZScore(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ZREM":
		resp, err := HandleZRem(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "XADD":
		resp, err := HandleXAdd(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, true, false
	case "XRANGE":
		resp, err := HandleXRange(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "XREAD":
		resp, err := HandleXRead(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "GET":
		resp, err := HandleGet(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "TYPE":
		resp, err := HandleType(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "KEYS":
		resp, err := HandleKeys(args, s.store)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "INFO":
		resp, err := HandleInfo(args, s.replica)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "CONFIG":
		resp, err := HandleConfig(args, s.config)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "ACL":
		currentUser := ""
		if authenticatedUser != nil {
			currentUser = *authenticatedUser
		}
		resp, err := HandleACL(args, currentUser)
		if err != nil {
			return EncodeError(err.Error()), false, false
		}
		return resp, false, false
	case "REPLCONF":
		return EncodeSimpleString("OK"), false, false
	case "PSYNC":
		conn.Write(EncodeSimpleString("FULLRESYNC 8371445fff36d3332a088d7be77bf1419d907b2d 0"))
		rdbData := emptyRDB()
		conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(rdbData))))
		conn.Write(rdbData)
		re := &replicaEntry{conn: conn, reader: reader}
		s.replicaMu.Lock()
		s.replicas = append(s.replicas, re)
		s.replicaMu.Unlock()
		go s.readReplicaACKs(re)
		return nil, false, true
	case "WAIT":
		return s.handleWait(args), false, false
	default:
		return EncodeError("ERR unknown command"), false, false
	}
}

func handlePing(args []string, inSubscribeMode bool) ([]byte, error) {
	if len(args) > 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments for 'ping' command")
	}

	if inSubscribeMode {
		message := ""
		if len(args) == 1 {
			message = args[0]
		}
		return EncodeRESPArray([][]byte{
			EncodeBulkString("pong"),
			EncodeBulkString(message),
		}), nil
	}

	if len(args) == 1 {
		return EncodeBulkString(args[0]), nil
	}

	return EncodeSimpleString("PONG"), nil
}

func isAllowedInSubscribeMode(command string) bool {
	switch command {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "SSUBSCRIBE", "SUNSUBSCRIBE", "PING", "QUIT", "RESET", "PUBLISH":
		return true
	default:
		return false
	}
}

func (s *Server) isSubscribedConnection(conn net.Conn) bool {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	channels, ok := s.connSubs[conn]
	return ok && len(channels) > 0
}

func (s *Server) handleSubscribe(conn net.Conn, channels []string) ([]byte, error) {
	if len(channels) == 0 {
		return nil, fmt.Errorf("ERR wrong number of arguments for 'subscribe' command")
	}

	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	if _, ok := s.connSubs[conn]; !ok {
		s.connSubs[conn] = make(map[string]struct{})
	}

	var out []byte
	for _, ch := range channels {
		if _, ok := s.channelSubs[ch]; !ok {
			s.channelSubs[ch] = make(map[net.Conn]struct{})
		}
		s.channelSubs[ch][conn] = struct{}{}
		s.connSubs[conn][ch] = struct{}{}

		count := len(s.connSubs[conn])
		out = append(out, encodeSubscribeAck(ch, count)...)
	}

	return out, nil
}

func (s *Server) handleUnsubscribe(conn net.Conn, channels []string) []byte {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	current, ok := s.connSubs[conn]
	if !ok {
		current = make(map[string]struct{})
		s.connSubs[conn] = current
	}

	var targets []string
	if len(channels) == 0 {
		targets = make([]string, 0, len(current))
		for ch := range current {
			targets = append(targets, ch)
		}
		sort.Strings(targets)
		if len(targets) == 0 {
			return encodeUnsubscribeAck(nil, 0)
		}
	} else {
		targets = channels
	}

	var out []byte
	for _, ch := range targets {
		if _, subscribed := current[ch]; subscribed {
			delete(current, ch)
			if subs, exists := s.channelSubs[ch]; exists {
				delete(subs, conn)
				if len(subs) == 0 {
					delete(s.channelSubs, ch)
				}
			}
		}

		count := len(current)
		channel := ch
		out = append(out, encodeUnsubscribeAck(&channel, count)...)
	}

	if len(current) == 0 {
		delete(s.connSubs, conn)
	}

	return out
}

func (s *Server) publish(channel, message string) int {
	s.pubsubMu.Lock()
	subs, ok := s.channelSubs[channel]
	if !ok || len(subs) == 0 {
		s.pubsubMu.Unlock()
		return 0
	}

	targets := make([]net.Conn, 0, len(subs))
	for c := range subs {
		targets = append(targets, c)
	}
	s.pubsubMu.Unlock()

	payload := encodePubSubMessage(channel, message)
	delivered := 0
	for _, c := range targets {
		if _, err := c.Write(payload); err != nil {
			s.unsubscribeConnection(c)
			continue
		}
		delivered++
	}

	return delivered
}

func (s *Server) unsubscribeConnection(conn net.Conn) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	channels, ok := s.connSubs[conn]
	if !ok {
		return
	}

	for ch := range channels {
		subs, exists := s.channelSubs[ch]
		if !exists {
			continue
		}
		delete(subs, conn)
		if len(subs) == 0 {
			delete(s.channelSubs, ch)
		}
	}
	delete(s.connSubs, conn)
}

func encodeSubscribeAck(channel string, count int) []byte {
	return EncodeRESPArray([][]byte{
		EncodeBulkString("subscribe"),
		EncodeBulkString(channel),
		EncodeInteger(int64(count)),
	})
}

func encodeUnsubscribeAck(channel *string, count int) []byte {
	channelPayload := EncodeNullBulkString()
	if channel != nil {
		channelPayload = EncodeBulkString(*channel)
	}

	return EncodeRESPArray([][]byte{
		EncodeBulkString("unsubscribe"),
		channelPayload,
		EncodeInteger(int64(count)),
	})
}

func encodePubSubMessage(channel, message string) []byte {
	return EncodeRESPArray([][]byte{
		EncodeBulkString("message"),
		EncodeBulkString(channel),
		EncodeBulkString(message),
	})
}

func (s *Server) Stop() {
	s.l.Close()
}

// propagate sends a write command as a RESP array to all connected replicas.
func (s *Server) propagate(parts []string) {
	data := EncodeArray(parts)
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	for _, r := range s.replicas {
		if _, err := r.conn.Write(data); err != nil {
			fmt.Println("Error propagating to replica:", err)
		}
	}
	s.masterOffset += int64(len(data))
}

// readReplicaACKs continuously reads REPLCONF ACK responses from a replica.
func (s *Server) readReplicaACKs(re *replicaEntry) {
	for {
		parts, err := ReadArray(re.reader)
		if err != nil {
			fmt.Println("Error reading ACK from replica:", err)
			re.conn.Close()
			return
		}
		if len(parts) == 3 && strings.ToUpper(parts[0]) == "REPLCONF" && strings.ToUpper(parts[1]) == "ACK" {
			off, err := strconv.ParseInt(parts[2], 10, 64)
			if err == nil {
				re.mu.Lock()
				re.ackOffset = off
				re.mu.Unlock()
			}
		}
	}
}

// handleWait implements the WAIT command: WAIT numreplicas timeout
func (s *Server) handleWait(args []string) []byte {
	if len(args) < 2 {
		return EncodeError("ERR wrong number of arguments for 'wait' command")
	}
	numReplicas, err := strconv.Atoi(args[0])
	if err != nil {
		return EncodeError("ERR value is not an integer")
	}
	timeoutMs, err := strconv.Atoi(args[1])
	if err != nil {
		return EncodeError("ERR value is not an integer")
	}

	s.replicaMu.Lock()
	currentOffset := s.masterOffset
	replicas := make([]*replicaEntry, len(s.replicas))
	copy(replicas, s.replicas)
	s.replicaMu.Unlock()

	// If nothing was propagated, all connected replicas are up to date
	if currentOffset == 0 {
		return EncodeInteger(int64(len(replicas)))
	}

	// Send REPLCONF GETACK * to all replicas
	getack := EncodeArray([]string{"REPLCONF", "GETACK", "*"})
	for _, r := range replicas {
		r.conn.Write(getack)
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)

	for time.Now().Before(deadline) {
		acked := 0
		for _, r := range replicas {
			r.mu.Lock()
			off := r.ackOffset
			r.mu.Unlock()
			if off >= currentOffset {
				acked++
			}
		}
		if acked >= numReplicas {
			return EncodeInteger(int64(acked))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Final count after timeout
	acked := 0
	for _, r := range replicas {
		r.mu.Lock()
		off := r.ackOffset
		r.mu.Unlock()
		if off >= currentOffset {
			acked++
		}
	}
	return EncodeInteger(int64(acked))
}

// emptyRDB returns the bytes of a minimal valid RDB file.
func emptyRDB() []byte {
	const emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfe0d694736"
	data, _ := hex.DecodeString(emptyRDBHex)
	return data
}
