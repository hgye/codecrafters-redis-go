package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/redis"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	dir := flag.String("dir", "", "directory for RDB files")
	dbfilename := flag.String("dbfilename", "", "RDB filename")
	port := flag.Int("port", 6379, "port to listen on")
	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", *port)
		os.Exit(1)
	}

	cfg := redis.Config{
		Dir:        *dir,
		DBFilename: *dbfilename,
	}
	server := redis.NewServer(l, cfg)
	server.Start()
	defer server.Stop()
}
