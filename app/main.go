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
	flag.Parse()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
