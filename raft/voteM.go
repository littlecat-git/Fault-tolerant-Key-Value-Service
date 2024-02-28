package raft

import (
    "math/rand"
    "time"
)

const electionTimeout = time.Millisecond * 180

type RequestVoteArgs struct {
        Term         int
        CandidateId  int
        LastLogIndex int
        LastLogTerm  int
}



type RequestVoteReply struct {
        Term        int
        VoteGranted bool
}

func (rf *Raft) getReceiverLastLogInfoL() (int, int) {
    receiverLastLogIndex := rf.log.lastIndex()
    lastLogEntry := rf.log.getEntry(receiverLastLogIndex,rf.me, "v1")
    receiverLastLogTerm := lastLogEntry.Term
    return receiverLastLogIndex, receiverLastLogTerm
}

func (rf *Raft) upToDateL(args *RequestVoteArgs) bool {
    LastLogIndex, LastLogTerm := rf.getReceiverLastLogInfoL()
    temp := false
    if args.LastLogTerm > LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogIndex >= LastLogIndex) {
        temp = true
    }
    return temp
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if args.Term < rf.currentTerm{
                reply.Term = rf.currentTerm
                reply.VoteGranted = false
                return
        }
        if args.Term > rf.currentTerm{
                rf.updateCurrentTermL(args.Term)
        }
        if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.upToDateL(args) {
                DebugF(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
                rf.votedFor = args.CandidateId
                rf.persist()
                rf.setElectionTimeL()

                DebugF(dTimer, "S%d Resetting ELT, received VoteRequest T%d", rf.me, rf.currentTerm)
                reply.VoteGranted = true
        }else{
                reply.VoteGranted = false
        }
        reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
        ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
        return ok
}


func (rf *Raft)setElectionTimeL(){
        t := time.Now()
        t = t.Add(electionTimeout)
        ms := rand.Int63() % 300
        t = t.Add(time.Duration(ms) * time.Millisecond)
        rf.electionTime = t
}

func (rf *Raft)startElectionL(){
        rf.currentTerm += 1
        rf.state = Candidate
        rf.votedFor = rf.me
        rf.persist()
        rf.requestVotesL()
}

func (rf *Raft)requestVotesL(){
	args := RequestVoteArgs{
        	Term:         rf.currentTerm,
	        CandidateId:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.getEntry(rf.log.lastIndex(),rf.me, "v2").Term,
	}
        votes := 1
        for i, _ := range rf.peers{
                if i!=rf.me{
                        go rf.requestVote(i, &args, &votes)
                }
        }
}

func (rf *Raft)becomeLeaderL(){
        rf.state = Leader
	
	DebugF(dError, "S%d set nextIndex[i] to %d", rf.me, rf.log.lastIndex() + 1)

	for i := 0; i < len(rf.peers); i++{
	       rf.nextIndex[i] = rf.log.lastIndex() + 1
        }

	//go rf.sendBatchedAppendsPeriodically()

}

func (rf *Raft)requestVote(peer int, args *RequestVoteArgs, votes *int){
        reply := RequestVoteReply{}
        ok := rf.sendRequestVote(peer, args, &reply)
        if ok{
                rf.mu.Lock()
                defer rf.mu.Unlock()
                if reply.Term > rf.currentTerm{
                        rf.updateCurrentTermL(reply.Term)
                }
                if reply.VoteGranted{
                        *votes += 1
                        DebugF(dVote, "S%d <- S%d Got vote", rf.me, peer)
                        if *votes > len(rf.peers)/2{
                                if rf.currentTerm == args.Term && rf.state == Candidate{
                                        DebugF(dLeader, "S%d Achieved Majority for T%d (%d), converting to Leader", rf.me, rf.currentTerm, *votes)
                                        rf.becomeLeaderL()
                                        rf.sendAppendsL(true)
                                }
                        }
                }
        }
}




