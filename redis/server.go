package redis

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
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
}

func NewServer(l net.Listener, cfg Config, replica *ReplicaInfo, port string) *Server {
	s := &Server{l: l, store: NewStore(), config: cfg, replica: replica, port: port}
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
		args := parts[1:]

		switch command {
		case "PING":
			conn.Write(EncodeSimpleString("PONG"))
		case "ECHO":
			resp, err := HandleEcho(args)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "SET":
			resp, err := HandleSet(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
			s.propagate(parts)
		case "INCR":
			resp, err := HandleIncr(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
			s.propagate(parts)
		case "XADD":
			resp, err := HandleXAdd(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
			s.propagate(parts)
		case "XRANGE":
			resp, err := HandleXRange(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "XREAD":
			resp, err := HandleXRead(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "GET":
			resp, err := HandleGet(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "TYPE":
			resp, err := HandleType(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "KEYS":
			resp, err := HandleKeys(args, s.store)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "INFO":
			resp, err := HandleInfo(args, s.replica)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "CONFIG":
			resp, err := HandleConfig(args, s.config)
			if err != nil {
				conn.Write(EncodeError(err.Error()))
				continue
			}
			conn.Write(resp)
		case "REPLCONF":
			conn.Write(EncodeSimpleString("OK"))
		case "PSYNC":
			conn.Write(EncodeSimpleString("FULLRESYNC 8371445fff36d3332a088d7be77bf1419d907b2d 0"))
			// Send empty RDB file
			rdbData := emptyRDB()
			conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(rdbData))))
			conn.Write(rdbData)
			// Register this connection as a replica and start ACK reader
			re := &replicaEntry{
				conn:   conn,
				reader: reader,
			}
			s.replicaMu.Lock()
			s.replicas = append(s.replicas, re)
			s.replicaMu.Unlock()
			// Connection is now a replication stream; stop processing commands
			go s.readReplicaACKs(re)
			return
		case "WAIT":
			resp := s.handleWait(args)
			conn.Write(resp)
		default:
			conn.Write(EncodeError("ERR unknown command"))
		}
	}
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
