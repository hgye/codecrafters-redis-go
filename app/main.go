package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type RESTPrefix string

const (
	arrayPrefix        RESTPrefix = "*"
	bulkStringPrefix   RESTPrefix = "$"
	simpleStringPrefix RESTPrefix = "+"
	errorPrefix        RESTPrefix = "-"
	integerPrefix      RESTPrefix = ":"
)

func (p RESTPrefix) String() string {
	return string(p)
}

func (p RESTPrefix) IsPrefix(line string) bool {
	return strings.HasPrefix(line, p.String())
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// Create a buffer for reading lines
	// go func(conn net.Conn) {

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading from connection:", err.Error())
			os.Exit(1)
		}

		if arrayPrefix.IsPrefix(line) {
			fmt.Println("Skipping RESP array length indicator:", line)
			// Skip RESP array length indicator
			line, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				os.Exit(1)
			}
			fmt.Println("Skipping RESP bulk string length:", line)
			// Skip RESP bulk string length
			line, err = reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading from connection:", err.Error())
				os.Exit(1)
			}
		}
		fmt.Printf("Received command: %s\n", line)
		// switch command := strings.TrimSpace(line);
		command := strings.TrimSpace(line)

		fmt.Printf("Received PING command, command length: %d, command: %s\n", len(command), command)
		switch strings.ToUpper(command) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
	// }(conn)
}
