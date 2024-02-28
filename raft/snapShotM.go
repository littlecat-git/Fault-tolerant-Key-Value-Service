package raft
//这里RPC


//先定义个结构体
type InstallSnapshotArgs struct {
	Term              int    // Term of the leader
	LeaderId          int // ID of the leader
	LastIncludedIndex int    // The snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // Term of LastIncludedIndex
	Data              []byte // Raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
        Term        int
	Success     bool 
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//其实快照可以看作某种形式的心跳；

        rf.mu.Lock()
        defer rf.mu.Unlock()
	//part 1 处理任期
	DebugF(dError, "S%d install snapshot from leader!!!", rf.me)
        if args.Term < rf.currentTerm{
                return
        }

	if args.Term > rf.currentTerm{
                 rf.updateCurrentTermL(args.Term)
        }
        rf.state = Follower
        rf.setElectionTimeL()
        DebugF(dTimer, "S%d Resetting ELT, received Snapshot T%d", rf.me, rf.currentTerm)
        reply.Term = rf.currentTerm


	
	if rf.snapshotIndex >= args.LastIncludedIndex{
		DebugF(dError,"S%d get outdate snapshot, end", rf.me)
		return
	}
	reply.Success = true

	rf.waitingSnapshot = args.Data
	rf.waitingIndex = args.LastIncludedIndex
	rf.waitingTerm = args.LastIncludedTerm
	rf.lastApplied = rf.waitingIndex

	rf.snapshot      = rf.waitingSnapshot
	rf.snapshotIndex = rf.waitingIndex
	rf.snapshotTerm  = rf.waitingTerm 
	
	rf.clearLogEntriesL(args)

}//也许这里的代码可以交给ai优化

func (rf *Raft) clearLogEntriesL(args *InstallSnapshotArgs) {
	rf.log.index0 = args.LastIncludedIndex
	if args.LastIncludedIndex >rf.log.start() &&
	args.LastIncludedIndex <= rf.log.lastIndex(){
		entry := rf.log.getEntry(args.LastIncludedIndex, rf.me, "clear")
		if entry != nil && entry.Term == args.LastIncludedTerm{
			rf.log.entry = rf.log.entry[args.LastIncludedIndex - rf.log.start():]
			return
		}
	}else{
		rf.log.entry = make([]logEntry, 1)
	}
	rf.log.entry[0].Term = args.LastIncludedTerm

}


func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
        ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
        return ok
}


func (rf *Raft) TransmitSnapshotToFollowersL(i int){
	args := InstallSnapshotArgs{
	     	Term:              rf.currentTerm,
                LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,   
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.snapshot,
        }
	go rf.sendSnap(i, &args)
}

func (rf *Raft) sendSnap( peer int, args *InstallSnapshotArgs){
        reply := InstallSnapshotReply{}
        ok := rf.sendSnapshot(peer, args, &reply)

        if ok{
                rf.mu.Lock()
                defer rf.mu.Unlock()
                if reply.Term > rf.currentTerm{
                        rf.updateCurrentTermL(reply.Term)
                }else if reply.Success == true{
			rf.sendAppendsL(false)
		}

        }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

//貌似挺简单地，1截断日志，2更新index0， 3保存snapshot
func (rf *Raft) Snapshot(index int, snapshot []byte) {
        // Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.log.start() || index > rf.log.lastIndex(){
		return
	}
	DebugF(dError, "S%d is making a snapshot", rf.me)
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log.getEntry(index, rf.me, "shot1").Term

	//避免prevLogIndex等于rf.log.start()时任期不匹配的问题
	rf.log.entry[0].Term = rf.snapshotTerm
	
	rf.log.cutStart(index)
	
	rf.persist()
}


