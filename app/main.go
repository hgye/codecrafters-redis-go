package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/redis"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	dir := flag.String("dir", "", "directory for RDB files")
	dbfilename := flag.String("dbfilename", "", "RDB filename")
	port := flag.Int("port", 6379, "port to listen on")
	replicaof := flag.String("replicaof", "", "replicate from <host port>")
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

	var replica *redis.ReplicaInfo
	if *replicaof != "" {
		parts := strings.SplitN(*replicaof, " ", 2)
		if len(parts) == 2 {
			replica = &redis.ReplicaInfo{
				MasterHost: parts[0],
				MasterPort: parts[1],
			}
		}
	}

	server := redis.NewServer(l, cfg, replica, fmt.Sprintf("%d", *port))
	server.Start()
	defer server.Stop()
}
