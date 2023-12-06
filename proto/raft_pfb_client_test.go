package proto

import (
	"context"
	"fmt"
	"github.com/yushikuann/go-raft-sdk/proto/raft_pfb"
	"google.golang.org/grpc"
	"testing"
)

func TestClient(t *testing.T) {
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", 20000), grpc.WithInsecure())
	if err != nil {
		fmt.Println("get err1 as:", err)
	}
	defer conn.Close()
	client := raft_pfb.NewRaftNodeClient(conn)

	request := &raft_pfb.RequestVoteArgs{
		From:         1,
		To:           2,
		Term:         3,
		CandidateId:  4,
		LastLogIndex: 5,
		LastLogTerm:  6,
	}

	reply, err := client.RequestVote(context.Background(), request)
	if err != nil {
		fmt.Println("get err2 as:", err)
	}
	fmt.Println(reply)
}
