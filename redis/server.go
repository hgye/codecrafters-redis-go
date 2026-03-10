package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type Config struct {
	Dir        string
	DBFilename string
}

type Server struct {
	l      net.Listener
	store  *Store
	config Config
}

func NewServer(l net.Listener, cfg Config) *Server {
	return &Server{l: l, store: NewStore(), config: cfg}
}

func (s *Server) Start() {
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
		case "GET":
			resp, err := HandleGet(args, s.store)
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
		default:
			conn.Write(EncodeError("ERR unknown command"))
		}
	}
}

func (s *Server) Stop() {
	s.l.Close()
}
