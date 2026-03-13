package redis

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
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

type Server struct {
	l          net.Listener
	store      *Store
	config     Config
	replica    *ReplicaInfo
	port       string
	masterConn net.Conn
	replicaMu  sync.Mutex
	replicas   []net.Conn
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
		case "GET":
			resp, err := HandleGet(args, s.store)
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
			// Register this connection as a replica
			s.replicaMu.Lock()
			s.replicas = append(s.replicas, conn)
			s.replicaMu.Unlock()
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
		if _, err := r.Write(data); err != nil {
			fmt.Println("Error propagating to replica:", err)
		}
	}
}

// emptyRDB returns the bytes of a minimal valid RDB file.
func emptyRDB() []byte {
	const emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfe0d694736"
	data, _ := hex.DecodeString(emptyRDBHex)
	return data
}
