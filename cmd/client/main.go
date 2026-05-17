package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"distkv/client"
)

const defaultEndpoints = "localhost:8001,localhost:8002,localhost:8003"

func main() {
	flags := flag.NewFlagSet("distkv-client", flag.ExitOnError)
	peers := flags.String("peers", defaultEndpoints, "comma-separated host:port endpoints")
	addr := flags.String("addr", "", "a single endpoint, overriding --peers")
	flags.Usage = usage
	_ = flags.Parse(os.Args[1:])

	args := flags.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	endpoints := splitEndpoints(*peers)
	if *addr != "" {
		endpoints = []string{*addr}
	}

	c := client.New(endpoints)
	defer c.Close()

	if err := dispatch(c, args[0], args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dispatch(c *client.Client, command string, args []string) error {
	switch command {
	case "set":
		if len(args) != 2 {
			return fmt.Errorf("usage: set <key> <value>")
		}
		if err := c.Set(args[0], args[1]); err != nil {
			return err
		}
		fmt.Println("OK")

	case "setex":
		if len(args) != 3 {
			return fmt.Errorf("usage: setex <key> <value> <ttl>")
		}
		ttl, err := time.ParseDuration(args[2])
		if err != nil {
			return fmt.Errorf("invalid ttl %q: %w", args[2], err)
		}
		if err := c.SetEx(args[0], args[1], ttl); err != nil {
			return err
		}
		fmt.Printf("OK (expires in %s)\n", ttl)

	case "get":
		if len(args) != 1 {
			return fmt.Errorf("usage: get <key>")
		}
		value, found, err := c.Get(args[0])
		if err != nil {
			return err
		}
		if !found {
			fmt.Println("NOT_FOUND")
			os.Exit(1)
		}
		fmt.Println(value)

	case "delete":
		if len(args) != 1 {
			return fmt.Errorf("usage: delete <key>")
		}
		if err := c.Delete(args[0]); err != nil {
			return err
		}
		fmt.Println("OK")

	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: list <prefix>")
		}
		keys, err := c.List(args[0])
		if err != nil {
			return err
		}
		for _, key := range keys {
			fmt.Println(key)
		}

	default:
		usage()
		os.Exit(2)
	}
	return nil
}

func splitEndpoints(spec string) []string {
	var endpoints []string
	for _, part := range strings.Split(spec, ",") {
		if part = strings.TrimSpace(part); part != "" {
			endpoints = append(endpoints, part)
		}
	}
	return endpoints
}

func usage() {
	fmt.Fprint(os.Stderr, `distkv-client - command-line client for a Distkv cluster

Usage:
  distkv-client [--peers <list> | --addr <endpoint>] <command> [arguments]

Commands:
  set     <key> <value>         store a value
  setex   <key> <value> <ttl>   store a value with a TTL (e.g. 3600s, 1h)
  get     <key>                 read a value
  delete  <key>                 remove a key
  list    <prefix>              list keys sharing a prefix

Flags:
  --peers   comma-separated host:port endpoints (default `+defaultEndpoints+`)
  --addr    a single endpoint, overriding --peers
`)
}
