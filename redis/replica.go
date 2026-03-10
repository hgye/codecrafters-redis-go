package redis

import (
	"bufio"
	"fmt"
	"net"
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
	s.masterConn = conn
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
