package node

import (
	"fmt"
	"github.com/yushikuann/go-raft-sdk/raft"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

func NewRaftNode(
	myPort int,
	nodeIdPortMap map[int]int,
	nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {

	delete(nodeIdPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		peers:                   hostConnectionMap,
		myId:                    int32(nodeId),
		role:                    raft.Role_Follower,
		leaderId:                -1,
		resetCurElectionTicker:  make(chan bool),
		stopCurElectionTicker:   make(chan bool),
		resetCurHeartBeatTicker: make(chan bool),
		stopCurHeartBeatTicker:  make(chan bool),
		currentTerm:             0,
		votedFor:                -1,
		kvStore:                 make(map[string]int32),
		commitIndex:             0,
		nextIndex:               make([]int32, len(nodeIdPortMap)+1),
		matchIndex:              make([]int32, len(nodeIdPortMap)+1),
		stopCurElection:         make(chan bool),
		waitingOp:               make(map[int32]chan bool),
	}
	rn.log = append(rn.log, &raft.LogEntry{Term: 0})
	for i := range rn.nextIndex {
		rn.nextIndex[i] = rn.getLastLogIndex() + 1
		rn.matchIndex[i] = 0
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myPort))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myPort)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeIdPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			log.Printf("Try connecting to port: %d", hostPorts)
			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("[%d]: Successfully connect all nodes", myPort)

	//TODO: kick off leader election here !
	go rn.ElectionTicker(electionTimeout)
	go rn.HeartBeatTicker(heartBeatInterval)
	go rn.run()

	return &rn, nil
}
