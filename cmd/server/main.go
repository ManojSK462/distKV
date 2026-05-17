package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"distkv/store"
)

func main() {
	id := flag.Int("id", 0, "unique numeric id of this node")
	peers := flag.String("peers", "", "cluster as comma-separated id@host:port entries, including this node")
	dataDir := flag.String("data-dir", "data", "directory for this node's persistent state")
	streamq := flag.String("streamq", "", "StreamQ broker address (host:port) to publish committed writes to; empty disables it")
	flag.Parse()

	if *id <= 0 || *peers == "" {
		flag.Usage()
		os.Exit(2)
	}

	cluster, err := parseCluster(*peers)
	if err != nil {
		log.Fatalf("distkv: invalid --peers: %v", err)
	}
	addr, ok := cluster[*id]
	if !ok {
		log.Fatalf("distkv: node id %d does not appear in --peers", *id)
	}

	distkv, err := store.NewDistkv(*id, cluster, *dataDir, *streamq)
	if err != nil {
		log.Fatalf("distkv: %v", err)
	}

	server := rpc.NewServer()
	if err := distkv.Register(server); err != nil {
		log.Fatalf("distkv: registering RPC services: %v", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("distkv: listening on %s: %v", addr, err)
	}
	go serve(listener, server)

	distkv.Start()
	log.Printf("distkv: node %d serving on %s (cluster of %d)", *id, addr, len(cluster))
	if *streamq != "" {
		log.Printf("distkv: node %d publishing committed writes to StreamQ at %s", *id, *streamq)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals

	log.Printf("distkv: node %d shutting down", *id)
	listener.Close()
	distkv.Stop()
}

func serve(listener net.Listener, server *rpc.Server) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go server.ServeConn(conn)
	}
}

func parseCluster(spec string) (map[int]string, error) {
	cluster := make(map[int]string)
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		at := strings.IndexByte(entry, '@')
		if at <= 0 || at == len(entry)-1 {
			return nil, fmt.Errorf("entry %q is not in id@host:port form", entry)
		}
		id, err := strconv.Atoi(entry[:at])
		if err != nil {
			return nil, fmt.Errorf("entry %q has a non-numeric id", entry)
		}
		if _, dup := cluster[id]; dup {
			return nil, fmt.Errorf("node id %d is listed more than once", id)
		}
		cluster[id] = entry[at+1:]
	}
	if len(cluster) == 0 {
		return nil, fmt.Errorf("no cluster members were specified")
	}
	return cluster, nil
}
