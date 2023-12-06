package node

import (
	"context"
	"fmt"
	"github.com/yushikuann/go-raft-sdk/proto/raft_pfb"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const DEBUG = false

func printf(format string, a ...interface{}) (n int, err error) {
	if DEBUG {
		log.Printf(format, a...)
	}
	return
}

type raftNode struct {
	// Generic info
	peers    map[int32]raft_pfb.RaftNodeClient
	myId     int32
	role     raft_pfb.Role
	mu       sync.Mutex
	leaderId int32

	// handle timer
	resetCurElectionTicker  chan bool
	stopCurElectionTicker   chan bool
	resetCurHeartBeatTicker chan bool
	stopCurHeartBeatTicker  chan bool
	heartBeatInterval       int

	// Persistent state on all servers
	currentTerm int32
	votedFor    int32
	log         []*raft_pfb.LogEntry

	// kv store
	kvStore map[string]int32

	// Volatile state on all servers
	commitIndex int32

	// Volatile state on leaders
	nextIndex  []int32
	matchIndex []int32

	// for leader
	notifyHearBeat chan bool
	waitingOp      map[int32]chan bool
	// for candidate
	stopCurElection chan bool
	raft_pfb.UnimplementedRaftNodeServer
}

func (rn *raftNode) getRole() raft_pfb.Role {
	rn.mu.Lock()
	role := rn.role
	rn.mu.Unlock()
	return role
}

func (rn *raftNode) setRole(role raft_pfb.Role) {
	rn.mu.Lock()
	rn.role = role
	rn.mu.Unlock()
}

func (rn *raftNode) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rn.currentTerm)
}

func (rn *raftNode) setCurrentTerm(term int32) {
	atomic.StoreInt32(&rn.currentTerm, term)
}

func (rn *raftNode) getLastLogIndex() int32 {
	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	rn.mu.Unlock()
	return lastLogIndex
}

func (rn *raftNode) getLastLogTerm() int32 {
	rn.mu.Lock()
	lastLogTerm := rn.log[len(rn.log)-1].Term
	rn.mu.Unlock()
	return lastLogTerm
}

func (rn *raftNode) getLastEntry() (int32, int32) {
	rn.mu.Lock()
	lastLogIndex := int32(len(rn.log) - 1)
	lastLogTerm := rn.log[len(rn.log)-1].Term
	rn.mu.Unlock()
	return lastLogIndex, lastLogTerm
}

func (rn *raftNode) getCommitIndex() int32 {
	return atomic.LoadInt32(&rn.commitIndex)
}

func (rn *raftNode) setCommitIndex(commitId int32) {
	atomic.StoreInt32(&rn.commitIndex, commitId)
}

func (rn *raftNode) getVotedFor() int32 {
	return atomic.LoadInt32(&rn.votedFor)
}

func (rn *raftNode) setVotedFor(candidateId int32) {
	atomic.StoreInt32(&rn.votedFor, candidateId)
}

func (rn *raftNode) run() {
	for {
		role := rn.getRole()
		switch role {
		case raft_pfb.Role_Follower:
			rn.HandleFollower()
		case raft_pfb.Role_Candidate:
			rn.HandleCandidate()
		case raft_pfb.Role_Leader:
			rn.HandleLeader()
		}
	}
}

func (rn *raftNode) HandleFollower() {
	fmt.Println("Follower Running")
}

func (rn *raftNode) HandleCandidate() {
	fmt.Println("Candidate Running")
}

func (rn *raftNode) HandleLeader() {
	rn.leaderId = rn.myId
	for index := range rn.nextIndex {
		rn.nextIndex[index] = rn.getLastLogIndex() + 1
		rn.matchIndex[index] = 0
	}

	quorumSize := (len(rn.peers)+1)/2 + 1

	respCh := make(chan *raft_pfb.AppendEntriesReply, len(rn.peers))
	rn.notifyHearBeat = make(chan bool, 1)
	rn.resetCurHeartBeatTicker <- true
	rn.notifyHearBeat <- true
	for rn.getRole() == raft_pfb.Role_Leader {
		select {
		case <-rn.notifyHearBeat:
			go rn.sendHeartBeat(respCh)
		case reply := <-respCh:
			if reply.Term > rn.getCurrentTerm() {
				rn.setRole(raft_pfb.Role_Follower)
				rn.setCurrentTerm(reply.Term)
				rn.setVotedFor(-1)
				return
			}

			rn.matchIndex[reply.From] = reply.MatchIndex
			if reply.Success {
				rn.nextIndex[reply.From] = reply.MatchIndex + 1
			} else {
				rn.nextIndex[reply.From] = rn.nextIndex[reply.From] - 1
			}

			commitId := rn.getCommitIndex()
			nCommited := 1
			nextCommitId := rn.getLastLogIndex()
			for _, matchId := range rn.matchIndex {
				if matchId > commitId {
					nCommited += 1
					if matchId < nextCommitId {
						nextCommitId = matchId
					}
				}
			}
			if nextCommitId > commitId && nCommited >= quorumSize && rn.log[nextCommitId].Term == rn.getCurrentTerm() {
				startCommitId := rn.getCommitIndex()
				rn.setCommitIndex(nextCommitId)
				endCommitId := rn.getCommitIndex()
				for i := startCommitId + 1; i <= endCommitId; i++ {
					status := false
					logEntry := rn.log[i]
					if logEntry.Op == raft_pfb.Operation_Put {
						rn.kvStore[logEntry.Key] = logEntry.Value
						status = true
					} else {
						_, status = rn.kvStore[logEntry.Key]
						delete(rn.kvStore, logEntry.Key)
					}
					_, ok := rn.waitingOp[i]
					if ok {
						rn.waitingOp[i] <- status
						delete(rn.waitingOp, i)
					}
				}
			}
		}
	}
}

func (rn *raftNode) StartLeaderElection() {
	// response channel
	respCh := make(chan *raft_pfb.RequestVoteReply, len(rn.peers))

	rn.setCurrentTerm(rn.getCurrentTerm() + 1)
	currentTerm := rn.getCurrentTerm()
	lastLogIndex, lastLogTerm := rn.getLastEntry()

	for nodeId, peer := range rn.peers {

		go func(nodeId int32, client raft_pfb.RaftNodeClient) {
			request := raft_pfb.RequestVoteArgs{
				From:         rn.myId,
				To:           nodeId,
				Term:         currentTerm,
				CandidateId:  rn.myId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, _ := client.RequestVote(context.Background(), &request)
			respCh <- reply
		}(nodeId, peer)
	}

	quorumSize := (len(rn.peers)+1)/2 + 1
	var grantedVotes int32 = 0
	// self vote
	rn.setVotedFor(rn.myId)
	atomic.AddInt32(&grantedVotes, 1)

	for rn.getRole() == raft_pfb.Role_Candidate {
		select {
		case reply := <-respCh:
			if reply.Term > rn.getCurrentTerm() {
				rn.setRole(raft_pfb.Role_Follower)
				rn.setCurrentTerm(reply.Term)
				rn.setVotedFor(-1)
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&grantedVotes, 1)
				if int(grantedVotes) >= quorumSize {
					rn.setRole(raft_pfb.Role_Leader)
					return
				}
			}
		case <-rn.stopCurElection:
			return
		}
	}
}

func (rn *raftNode) sendHeartBeat(respCh chan *raft_pfb.AppendEntriesReply) {
	currentTerm := rn.getCurrentTerm()
	commitIndex := rn.getCommitIndex()

	for nodeId, peer := range rn.peers {

		go func(nodeId int32, client raft_pfb.RaftNodeClient) {
			prevLogEntry := rn.log[rn.nextIndex[nodeId]-1]
			EntryArgs := raft_pfb.AppendEntriesArgs{
				From:         rn.myId,
				To:           nodeId,
				Term:         currentTerm,
				LeaderId:     rn.myId,
				PrevLogIndex: rn.nextIndex[nodeId] - 1,
				PrevLogTerm:  prevLogEntry.Term,
				Entries:      rn.log[rn.nextIndex[nodeId]:],
				LeaderCommit: commitIndex,
			}
			printf("[%d] send heart beat: %v, log: %v, next: %v", rn.myId, EntryArgs, rn.log, rn.nextIndex)
			reply, _ := client.AppendEntries(context.Background(), &EntryArgs)
			printf("[%d] get heart beat reply: %v, next: %d, match: %d", rn.myId, *reply, rn.nextIndex[nodeId], rn.matchIndex[nodeId])
			respCh <- reply
		}(nodeId, peer)
	}
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft_pfb.ProposeArgs) (*raft_pfb.ProposeReply, error) {
	// TODO: Implement this!
	log.Printf("[%d] Receive propose from client: %v", rn.myId, *args)
	var ret raft_pfb.ProposeReply
	ret.CurrentLeader = rn.leaderId
	if rn.getRole() != raft_pfb.Role_Leader {
		ret.Status = raft_pfb.Status_WrongNode
	} else {
		logEntry := raft_pfb.LogEntry{
			Term:  rn.getCurrentTerm(),
			Op:    args.Op,
			Key:   args.Key,
			Value: args.V,
		}

		rn.log = append(rn.log, &logEntry)
		waitId := rn.getLastLogIndex()
		rn.waitingOp[waitId] = make(chan bool)
		select {
		case status := <-rn.waitingOp[waitId]:
			if status {
				ret.Status = raft_pfb.Status_OK
			} else {
				ret.Status = raft_pfb.Status_KeyNotFound
			}
		}
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft_pfb.GetValueArgs) (*raft_pfb.GetValueReply, error) {
	// TODO: Implement this!
	var ret raft_pfb.GetValueReply
	v, success := rn.kvStore[args.Key]
	if success {
		ret.V = v
		ret.Status = raft_pfb.Status_KeyFound
	} else {
		ret.Status = raft_pfb.Status_KeyNotFound
	}
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft_pfb.RequestVoteArgs) (*raft_pfb.RequestVoteReply, error) {
	// TODO: Implement this!
	currentTerm := rn.getCurrentTerm()
	reply := raft_pfb.RequestVoteReply{
		From:        args.To,
		To:          args.From,
		Term:        currentTerm,
		VoteGranted: false,
	}

	if args.Term < currentTerm {
		return &reply, nil
	}

	if args.Term > currentTerm {
		rn.setRole(raft_pfb.Role_Follower)
		rn.setCurrentTerm(args.Term)
		rn.setVotedFor(-1)
		reply.Term = args.Term
	}

	lastLogIndex, lastLogTerm := rn.getLastEntry()
	votedFor := rn.getVotedFor()

	up_to_date := (lastLogTerm > args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex)
	if (votedFor == -1 || votedFor == args.CandidateId) && !up_to_date {
		rn.setVotedFor(args.CandidateId)
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		rn.resetCurElectionTicker <- true
	}
	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft_pfb.AppendEntriesArgs) (*raft_pfb.AppendEntriesReply, error) {
	// TODO: Implement this
	currentTerm := rn.getCurrentTerm()
	reply := raft_pfb.AppendEntriesReply{
		From:       args.To,
		To:         args.From,
		Term:       currentTerm,
		Success:    false,
		MatchIndex: rn.getCommitIndex(),
	}

	if args.Term < currentTerm {
		return &reply, nil
	}

	rn.resetCurElectionTicker <- true
	rn.leaderId = args.LeaderId

	if (args.Term > currentTerm) || (rn.getRole() != raft_pfb.Role_Follower) {
		rn.setRole(raft_pfb.Role_Follower)
		rn.setCurrentTerm(args.Term)
		rn.setVotedFor(-1)
		reply.Term = args.Term
	}

	lastLogIndex, _ := rn.getLastEntry()

	log_match := lastLogIndex >= args.PrevLogIndex && rn.log[args.PrevLogIndex].Term == args.PrevLogTerm
	if !log_match {
		return &reply, nil
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + int32(i) + 1
		if index > lastLogIndex {
			rn.log = append(rn.log, entry)
		} else if rn.log[index].Term != entry.Term {
			rn.log = rn.log[:index]
			rn.log = append(rn.log, args.Entries[index])
		}
	}

	printf(">>> [%d] leaderCommit: %d, myCommit: %d, log: %v, lastLogId: %d", rn.myId, args.LeaderCommit, rn.getCommitIndex(), rn.log, rn.getLastLogIndex())
	if (args.LeaderCommit > rn.getCommitIndex()) && (rn.getLastLogIndex() > rn.getCommitIndex()) {
		startCommitId := rn.getCommitIndex()
		if args.LeaderCommit < rn.getLastLogIndex() {
			rn.setCommitIndex(args.LeaderCommit)
		} else {
			rn.setCommitIndex(rn.getLastLogIndex())
		}
		endCommitId := rn.getCommitIndex()
		printf(">>> [%d] comit from %d to %d, logs: %v", rn.myId, startCommitId, endCommitId, rn.log)
		for i := startCommitId + 1; i <= endCommitId; i++ {
			logEntry := rn.log[i]
			if logEntry.Op == raft_pfb.Operation_Put {
				rn.kvStore[logEntry.Key] = logEntry.Value
			} else {
				delete(rn.kvStore, logEntry.Key)
			}
			printf("[%d] store: %v, commit %v", rn.myId, rn.kvStore, logEntry)
		}
	}
	reply.MatchIndex = rn.getLastLogIndex()
	reply.Success = true
	return &reply, nil
}

func (rn *raftNode) RandomElectionTimeout(electionTimeoutTime int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randTime := r.Intn(90) // electionTimeoutTime)
	timeout := time.Duration(electionTimeoutTime+randTime) * time.Millisecond
	return timeout
}

func (rn *raftNode) ElectionTicker(electionTimeout int) {
	for {
		dur := rn.RandomElectionTimeout(electionTimeout)
		select {
		case <-rn.resetCurElectionTicker:
			continue
		case <-rn.stopCurElectionTicker:
			return
		case <-time.After(dur):
			role := rn.getRole()
			if role != raft_pfb.Role_Leader {
				if role != raft_pfb.Role_Candidate {
					rn.setRole(raft_pfb.Role_Candidate)
				} else {
					rn.stopCurElection <- true
				}
				go rn.StartLeaderElection()
			}
		}
	}
}

func (rn *raftNode) HeartBeatTicker(heartBeatInterval int) {
	rn.heartBeatInterval = heartBeatInterval
	for {
		select {
		case <-rn.stopCurHeartBeatTicker:
			return
		case <-rn.resetCurHeartBeatTicker:
			continue
		case <-time.After(time.Duration(heartBeatInterval) * time.Millisecond):
			role := rn.getRole()
			if role == raft_pfb.Role_Leader {
				rn.notifyHearBeat <- true
			}
		}
	}
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft_pfb.SetElectionTimeoutArgs) (*raft_pfb.SetElectionTimeoutReply, error) {
	// TODO: Implement this!
	// printf("[%d] reset timeout to %d ms", rn.myId, args.Timeout)
	rn.stopCurElectionTicker <- true
	go rn.ElectionTicker(int(args.Timeout))

	var reply raft_pfb.SetElectionTimeoutReply
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatTimeOUT(ctx context.Context, args *raft_pfb.SetHeartBeatIntervalArgs) (*raft_pfb.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!
	// printf("[%d] reset heartbeat to %d ms", rn.myId, args.Interval)
	rn.stopCurHeartBeatTicker <- true
	go rn.HeartBeatTicker(int(args.Interval))

	var reply raft_pfb.SetHeartBeatIntervalReply
	return &reply, nil
}

// NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft_pfb.CheckEventsArgs) (*raft_pfb.CheckEventsReply, error) {
	return nil, nil
}
