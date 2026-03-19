package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// Handshake performs the replication handshake with the master:
//  1. PING
//  2. REPLCONF listening-port <port>
//  3. REPLCONF capa psync2
//  4. PSYNC ? -1
func (s *Server) Handshake() error {
	addr := net.JoinHostPort(s.replica.MasterHost, s.replica.MasterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connect to master %s: %w", addr, err)
	}

	reader := bufio.NewReader(conn)

	// Step 1: PING
	if _, err := conn.Write(EncodeArray([]string{"PING"})); err != nil {
		conn.Close()
		return fmt.Errorf("send PING: %w", err)
	}
	if err := expectSimpleString(reader, "PONG"); err != nil {
		conn.Close()
		return fmt.Errorf("PING handshake: %w", err)
	}

	// Step 2: REPLCONF listening-port
	if _, err := conn.Write(EncodeArray([]string{"REPLCONF", "listening-port", s.port})); err != nil {
		conn.Close()
		return fmt.Errorf("send REPLCONF listening-port: %w", err)
	}
	if err := expectSimpleString(reader, "OK"); err != nil {
		conn.Close()
		return fmt.Errorf("REPLCONF listening-port: %w", err)
	}

	// Step 3: REPLCONF capa psync2
	if _, err := conn.Write(EncodeArray([]string{"REPLCONF", "capa", "psync2"})); err != nil {
		conn.Close()
		return fmt.Errorf("send REPLCONF capa: %w", err)
	}
	if err := expectSimpleString(reader, "OK"); err != nil {
		conn.Close()
		return fmt.Errorf("REPLCONF capa: %w", err)
	}

	// Step 4: PSYNC ? -1 (full resync)
	if _, err := conn.Write(EncodeArray([]string{"PSYNC", "?", "-1"})); err != nil {
		conn.Close()
		return fmt.Errorf("send PSYNC: %w", err)
	}
	resp, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return fmt.Errorf("read PSYNC response: %w", err)
	}
	resp = strings.TrimSpace(resp)
	if !strings.HasPrefix(resp, "+FULLRESYNC") {
		conn.Close()
		return fmt.Errorf("expected FULLRESYNC, got: %q", resp)
	}

	fmt.Println("Handshake complete:", resp)

	// Step 5: Receive RDB file from master
	if err := s.receiveRDB(reader); err != nil {
		conn.Close()
		return fmt.Errorf("receive RDB: %w", err)
	}

	s.masterConn = conn
	s.masterReader = reader
	return nil
}

// listenToMaster reads propagated commands from the master and applies them to the store.
func (s *Server) listenToMaster() {
	for {
		parts, err := ReadArray(s.masterReader)
		if err != nil {
			fmt.Println("Error reading from master:", err)
			s.masterConn.Close()
			return
		}
		if len(parts) == 0 {
			continue
		}

		// Calculate the byte size of this command for offset tracking
		cmdBytes := len(EncodeArray(parts))

		command := strings.ToUpper(parts[0])
		args := parts[1:]

		switch command {
		case "SET":
			if _, err := HandleSet(args, s.store); err != nil {
				fmt.Println("Error applying SET from master:", err)
			}
		case "XADD":
			if _, err := HandleXAdd(args, s.store); err != nil {
				fmt.Println("Error applying XADD from master:", err)
			}
		case "DEL":
			for _, key := range args {
				s.store.Delete(key)
			}
		case "REPLCONF":
			if len(args) >= 2 && strings.ToUpper(args[0]) == "GETACK" {
				ack := EncodeArray([]string{"REPLCONF", "ACK", fmt.Sprintf("%d", s.replOffset)})
				if _, err := s.masterConn.Write(ack); err != nil {
					fmt.Println("Error sending ACK to master:", err)
				}
			}
		default:
			fmt.Printf("Replica ignoring propagated command: %s\n", command)
		}

		s.replOffset += int64(cmdBytes)
	}
}

// receiveRDB reads the RDB bulk transfer ($<len>\r\n<bytes>) and parses it into the store.
func (s *Server) receiveRDB(reader *bufio.Reader) error {
	// Read the bulk string header: $<length>\r\n
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read RDB header: %w", err)
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "$") {
		return fmt.Errorf("expected RDB bulk string, got: %q", line)
	}
	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return fmt.Errorf("parse RDB length: %w", err)
	}

	// Read exactly <length> bytes of RDB data (no trailing \r\n for file transfer)
	rdbData := make([]byte, length)
	if _, err := io.ReadFull(reader, rdbData); err != nil {
		return fmt.Errorf("read RDB data: %w", err)
	}

	fmt.Printf("Received RDB file: %d bytes\n", length)

	// Parse the RDB content into the store
	if err := parseRDB(bytes.NewReader(rdbData), s.store); err != nil {
		return fmt.Errorf("parse RDB: %w", err)
	}

	return nil
}

func expectSimpleString(r *bufio.Reader, expected string) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	if line != "+"+expected {
		return fmt.Errorf("expected +%s, got %q", expected, line)
	}
	return nil
}
