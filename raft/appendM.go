package raft
//import "time"
type AppendEntriesArgs struct { Term         int // Leader's current term
        LeaderId     int // Leader ID
        PrevLogIndex int // Index of log entry immediately preceding new ones
        PrevLogTerm  int // Term of `PrevLogIndex` entry
        Entries      []logEntry // Log entries to store (empty for heartbeat)
        LeaderCommit int // Leader's commit index
}
type AppendEntriesReply struct{
        Term         int
        Success      bool //如果不匹配或是过期都有可能导致false
	XTerm        int  //term in the conflicting entry (if any)
	XIndex	     int  //index of first entry with that term (if any)
	XLen	     int  //log length
	Snapshot     int  //如果prevLogIndex发过来时由于自身已经截断相关日志，所以告知Leader下次要发个快照过来
}
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
func max(a, b int) int{
	if a > b{
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
        rf.mu.Lock()
        defer rf.mu.Unlock()
	DebugF(dTimer, "S%d see entries:%v", rf.me, args.Entries)
	DebugF(dTimer, "S%d see log:%v", rf.me, rf.log)
        if args.Term < rf.currentTerm {
                reply.Success = false
                reply.Term = rf.currentTerm
                return
        }
        if args.Term > rf.currentTerm{
		 rf.updateCurrentTermL(args.Term)
        }
        rf.state = Follower
        rf.setElectionTimeL()
        DebugF(dTimer, "S%d Resetting ELT, received AppEnt T%d", rf.me, rf.currentTerm)


        reply.Term = rf.currentTerm

	//follower的日志条目太短
	if args.PrevLogIndex >= rf.log.lastIndex() + 1 {

		//case3:
                reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.log.lastIndex() + 1
                return

	}else{
		temp := rf.log.getEntry(args.PrevLogIndex,rf.me, "u1")
		if temp == nil{
			//prevLogIndex低于log.start()
			reply.Success = false
			reply.XTerm = -1
			reply.XLen = rf.log.start() + 1
			return

			//下面则是发生冲突的情况
		}else{
			if temp.Term != args.PrevLogTerm{
				reply.Success = false
				reply.XTerm = temp.Term
                		reply.XIndex = findTerm(rf.log, reply.XTerm, true)
				return
			}
		}
	}



        reply.Success = true
	rf.replicateLogEntriesL(args)
	rf.followerUpdateCommitIndexAndNotifyL(args)
        return
}

func (rf *Raft) replicateLogEntriesL(args *AppendEntriesArgs) {
	conflictIndex := args.PrevLogIndex + 1
	tag := false
        temp := 0
        for i := 0; i < len(args.Entries); i++{
                temp = conflictIndex + i

	       if temp >= rf.log.lastIndex() + 1{

		        rf.log.entry = append(rf.log.entry, args.Entries[i:]...)

			tag = true
                        break



		}else if rf.log.getEntry(temp,rf.me, "u3").Term != args.Entries[i].Term{

			rf.log.entry = append(rf.log.entry[:temp-rf.log.index0], args.Entries[i:]...)

			tag = true
                        break
                }
		
        }
	if tag{ 	//避免不必要的开销
        	rf.persist()
	}
}

func (rf *Raft) followerUpdateCommitIndexAndNotifyL(args *AppendEntriesArgs) {
        oldcmi := rf.commitIndex
        if args.LeaderCommit > rf.commitIndex{
                rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries) )
        }
        newcmi := rf.commitIndex

        if oldcmi != newcmi{
                rf.applyCond.Signal()
                DebugF(dInfo, "S%d, old cmi:%d, new cmi:%d", rf.me, oldcmi, newcmi)
        }
	return 
}



func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
        return ok
}



func (rf *Raft)sendAppendsL(heartbeat bool){
        for i, _ := range rf.peers{
                if i!= rf.me{
			prevLogIndex := rf.nextIndex[i]-1
			if prevLogIndex < 0 || prevLogIndex >= rf.log.lastIndex() + 1 {
			    DebugF(dError, "S%d want to send to S%d, but prevLogIndex is %d, and self long is %d", rf.me, i, prevLogIndex, rf.log.lastIndex() + 1)
			    continue
			}
			
			if prevLogIndex < rf.log.start(){
				DebugF(dSnap,"S%d sent snapshot to S%d", rf.me, i)
				rf.nextIndex[i] = rf.log.start() + 1
				rf.TransmitSnapshotToFollowersL(i)
				continue
			}

			prevLogTerm := rf.log.getEntry(prevLogIndex, rf.me,"u4").Term
			entries := make([]logEntry, rf.log.lastIndex() - prevLogIndex)

			if prevLogIndex < rf.log.lastIndex() {
				copy(entries, rf.log.slice(prevLogIndex + 1) )
			}
			args := AppendEntriesArgs{
				Term: rf.currentTerm, 
				LeaderId: rf.me,
				PrevLogIndex :prevLogIndex,
				PrevLogTerm :prevLogTerm,
				Entries: entries,
				LeaderCommit: rf.commitIndex,
			}

			if heartbeat == false{
				DebugF(dLog, "S%d -> S%d Sending PLI: %d PLT: %d T: %d LC: %d, fc:remained", rf.me, i, prevLogIndex, prevLogTerm, rf.currentTerm, rf.commitIndex)

			}
                        go rf.sendAppend(i, heartbeat, &args)
                }
        }
}




func (rf *Raft) sendAppend(peer int, heartbeat bool, args *AppendEntriesArgs) {
        reply := AppendEntriesReply{}
        ok := rf.sendAppendEntries(peer, args, &reply)
	if ok {
            rf.mu.Lock()
            defer rf.mu.Unlock()
            rf.processAppendReplyL(peer, args, &reply)
        }
}



//处理对AppendEntries的回复
func (rf *Raft)processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply){
        if reply.Term > rf.currentTerm{
                rf.updateCurrentTermL(reply.Term)
        }else if rf.currentTerm == args.Term{
                rf.processAppendReplyTermL(peer, args, reply)
        }
}





func findTerm(log Log, targetTerm int, findLeftmost bool) int {
    //传入的第三个参数为false时，表示查找右边第一个符合的
    //为true时，表示查找左边第一个符合的
    left, right := 1, len(log.entry) - 1
    result := -1

    for left <= right {
        mid := left + (right-left)/2

        if log.entry[mid].Term == targetTerm {
            result = mid
            if findLeftmost {
                right = mid - 1 // 向左缩小搜索范围
            } else {
                left = mid + 1 // 向右缩小搜索范围
            }
        } else if log.entry[mid].Term < targetTerm {
            left = mid + 1 // 向右缩小搜索范围
        } else {
            right = mid - 1 // 向左缩小搜索范围
        }
    }

    return result
}




func (rf *Raft)processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply){
        if reply.Success == true{
		rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex + len(args.Entries) )
		rf.nextIndex[peer] = max(rf.nextIndex[peer], args.PrevLogIndex + len(args.Entries) + 1 )
		if rf.nextIndex[peer] <= 0 {
			DebugF(dError, "S%d w1 something really weird, nextIndex:%d", rf.me, rf.nextIndex[peer])
		}
        }else if reply.XTerm != -1{
		conflictIndex := -1
		conflictIndex = findTerm(rf.log, reply.XTerm, false)
		//如果找到了对应的条目：case2
		if conflictIndex != -1 {
			rf.nextIndex[peer] = conflictIndex
			if rf.nextIndex[peer] <= 0 {
                                DebugF(dError, "S%d w2 something really weird, nextIndex:%d", rf.me, rf.nextIndex[peer])
                        }
		}else{//找不到对应的条目，case1
			rf.nextIndex[peer] = reply.XIndex
			if rf.nextIndex[peer] <= 0 {
				DebugF(dError, "S%d w3 something really weird, nextIndex:%d", rf.me, rf.nextIndex[peer])
                	}
		}

        }else{
		//case3 follower的日志太短
		rf.nextIndex[peer] = reply.XLen 
		if rf.nextIndex[peer] <= 0 {
                        DebugF(dError, "S%d w4 something really weird, nextIndex:%d", rf.me, rf.nextIndex[peer])
                }
	}
	rf.leaderUpdateCommitIndexAndNotifyL(args)
        return
}

func (rf *Raft) leaderUpdateCommitIndexAndNotifyL(args *AppendEntriesArgs) {
	//更新commitIndex
        oldcmi:=rf.commitIndex
        tag := false
        halfPeers := len(rf.peers) /2
	for N := rf.log.lastIndex(); N > rf.commitIndex && N > rf.log.start(); N-- {

                count := 1
                for i := range rf.peers {
                        if i == rf.me{
                                continue
                        }

			if rf.matchIndex[i] >= N {
                                temp := rf.log.getEntry(N, rf.me, "u5")
                                if temp == nil{
                                        DebugF(dError, "S%d due to u5 fail", rf.me)
                                }else{
                                        if temp.Term == rf.currentTerm{
                                                count ++
                                                if count > halfPeers{
                                                        rf.commitIndex = N
                                                        tag = true
                                                        break
                                                }
                                        }


                                }

                        }


		}//end for

		if tag == true{
			break
		}//跳出外层循环


        }

	
        newcmi:=rf.commitIndex
        if tag == true{
		rf.sendAppendsL(false)
                rf.applyCond.Signal()
                DebugF(dInfo, "S%d, old cmi:%d, new cmi:%d", rf.me, oldcmi, newcmi)
        }
}


