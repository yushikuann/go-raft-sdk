package main

import (
	"github.com/yushikuann/go-raft-sdk/node"
	"log"
	"strconv"
	"strings"
)

func main() {
	//ports := os.Args[2]
	//myPort, _ := strconv.Atoi(os.Args[1])
	//nodeID, _ := strconv.Atoi(os.Args[3])
	//heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	//electionTimeout, _ := strconv.Atoi(os.Args[5])

	ports := "10001,10002,10003"
	myPort := 10001
	nodeID := 1
	heartBeatInterval := 100
	electionTimeout := 100

	portStrings := strings.Split(ports, ",")

	nodeIdPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeIdPortMap[i+1] = port
	}

	// Create and start the Raft Node.
	_, err := node.NewRaftNode(myPort, nodeIdPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}
