package proto

import (
	"context"
	"fmt"
	"github.com/yushikuann/go-raft-sdk/proto/raft_pfb"
	"google.golang.org/grpc"
	"net"
	"testing"
)

type raftNodeServer struct {
	raft_pfb.UnimplementedRaftNodeServer
}

func (r *raftNodeServer) Propose(ctx context.Context, args *raft_pfb.ProposeArgs) (*raft_pfb.ProposeReply, error) {
	//TODO implement me
	panic("implement me")
}

func (r *raftNodeServer) GetValue(ctx context.Context, args *raft_pfb.GetValueArgs) (*raft_pfb.GetValueReply, error) {
	//TODO implement me
	panic("implement me")
}

func (r *raftNodeServer) SetElectionTimeout(ctx context.Context, args *raft_pfb.SetElectionTimeoutArgs) (*raft_pfb.SetElectionTimeoutReply, error) {
	//TODO implement me
	panic("implement me")
}

func (r *raftNodeServer) SetHeartBeatInterval(ctx context.Context, args *raft_pfb.SetHeartBeatIntervalArgs) (*raft_pfb.SetHeartBeatIntervalReply, error) {
	//TODO implement me
	panic("implement me")
}

func (r *raftNodeServer) RequestVote(ctx context.Context, args *raft_pfb.RequestVoteArgs) (*raft_pfb.RequestVoteReply, error) {
	fmt.Println("requesting vote")
	reply := &raft_pfb.RequestVoteReply{
		From:        1000,
		To:          2000,
		Term:        3000,
		VoteGranted: true,
	}
	return reply, nil
}

func (r *raftNodeServer) AppendEntries(ctx context.Context, args *raft_pfb.AppendEntriesArgs) (*raft_pfb.AppendEntriesReply, error) {
	//TODO implement me
	panic("implement me")
}

func (r *raftNodeServer) CheckEvents(ctx context.Context, args *raft_pfb.CheckEventsArgs) (*raft_pfb.CheckEventsReply, error) {
	//TODO implement me
	panic("implement me")
}

func TestServer(t *testing.T) {
	st := raftNodeServer{}
	server := grpc.NewServer()
	raft_pfb.RegisterRaftNodeServer(server, &st)
	listener, err := net.Listen("tcp", ":20000")
	fmt.Println("服务器已启动，正在监听端口 20000")
	err = server.Serve(listener)
	if err != nil {
		fmt.Println("服务器启动失败:", err)
		return
	}
	select {}
}
