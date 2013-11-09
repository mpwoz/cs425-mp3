/*
 The main command-line program for joining the group and running queries on the hashtable
 Once started, allows user to type commands like 'insert k,v'
 as well as querying the ring
*/

package main

import (
	"./logger"
	"./ring"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

// The operations
func insert(key, val string) {
}

func update(key, val string) {
}

func remove(key string) {
}

func lookup(key string) {
}

func main() {

	var (
		listenPort     string
		groupMember    string
		faultTolerance int
	)

	flag.StringVar(&listenPort, "listen", "4567", "port to bind for UDP listener")
	flag.StringVar(&groupMember, "g", "", "address of an existing group member")
	flag.IntVar(&faultTolerance, "f", 0, "Use fault tolerance")
	flag.Parse()

	log.Println("Start server on port", listenPort)
	log.Println("Fault Tolernace", faultTolerance)

	hostPort := getHostPort(listenPort)

	//logger.Log("INFO", "Start Server on Port"+listenPort)

	//Add itself to the usertable - join
	ring, err := ring.NewMember(hostPort, faultTolerance)

	firstInGroup := groupMember == ""
	if !firstInGroup {
		ring.JoinGroup(groupMember)
		logger.Log("JOIN", "Gossiping new member to the group")
	} else {
		ring.FirstMember(hostPort)
		go ring.Gossip()
	}

	//UDP
	go ring.ReceiveDatagrams(firstInGroup)

	if err != nil {
		fmt.Println("Ring addition failed:", err)
		logger.Log("FAILURE", "Ring could not be created")
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		words := strings.Split(line, " ")
		var key, val string
		key = words[1]
		if len(words) > 2 {
			val = words[2]
		}
		switch words[0] {
		case "insert":
			insert(key, val)
		case "update":
			update(key, val)
		case "remove":
			remove(key)
		case "lookup":
			lookup(key)
		case "leave":
			fmt.Println("Leaving Group")
			ring.LeaveGroup()
		}

	}

	if err := scanner.Err(); err != nil {
		log.Panic("Scanning stdin", err)
	}

}

func getHostPort(port string) (hostPort string) {

	name, err := os.Hostname()
	if err != nil {
		fmt.Printf("Oops: %v\n", err)
		return
	}
	addrs, err := net.LookupHost(name)
	if err != nil {
		fmt.Printf("Oops: %v\n", err)
		return
	}

	hostPort = net.JoinHostPort(addrs[0], port)
	return hostPort
}
