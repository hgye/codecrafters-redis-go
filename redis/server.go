package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type Server struct {
	// conn net.Conn
	l net.Listener
}

func NewServer(l net.Listener) *Server {
	return &Server{l: l}
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
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			// os.Exit(1)
			conn.Close()
			return
		}

		if ArrayPrefix.IsPrefix(line) {
			fmt.Println("Skipping RESP array length indicator:", line)
			// Skip RESP array length indicator
			line, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				// os.Exit1)
				conn.Close()
				return
			}
			fmt.Println("Skipping RESP bulk string length:", line)
			// Skip RESP bulk string length
			line, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				// os.Exit(1)
				conn.Close()
				return
			}
		}
		fmt.Printf("Received command: %s\n", line)
		command := strings.TrimSpace(line)

		fmt.Printf("Received PING command, command length: %d, command: %s\n", len(command), command)
		switch strings.ToUpper(command) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func (s *Server) Stop() {
	s.l.Close()
}
