package raft
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labgob"
	"6.5840/labrpc" 
)

type State int
const (
	Follower    State = 0
	Candidate   State = 1
	Leader	    State = 2 
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()


	//Persistent state on all servers:
	currentTerm int
	votedFor    int

	//M log         []logEntry
	log Log

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	//Volatile state on leaders:
	nextIndex   []int
	matchIndex  []int

	//about leader election 
	state       State      //表状态,follower、candidate、leader
	electionTime time.Time //选举超时时间

	applyCh chan ApplyMsg
	applyCond *sync.Cond


	//Snapshot state
	snapshot []byte
	snapshotIndex int
	snapshotTerm int

	//Temorary location to give the service snapshot to the apply thread
	waitingSnapshot []byte
	waitingIndex int          //lastIncludedIndex
	waitingTerm int  	  //lastIncludedTerm

}



// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	//将Raft的持久状态编码为二进制格式并保存到稳定存储中。
	//使用Go语言提供的labgob包将Raft的持久状态进行编码
	w := new(bytes.Buffer)
        e := labgob.NewEncoder(w)
        e.Encode(rf.currentTerm)
        e.Encode(rf.votedFor)
        e.Encode(rf.log.entry)
	e.Encode(rf.log.index0)

        data := w.Bytes()
	//在实现快照之后，第二个参数应该为快照。
        rf.persister.Save(data, rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	//从稳定存储中读取之前保存的二进制格式的持久化状态。
	//使用labgob包对二进制数据进行解码，并将解码后的结果赋值给当前的Raft实例

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)

        d := labgob.NewDecoder(r)

        var currentTerm int
	var votedFor int
	var entry []logEntry
	var index0 int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&entry) != nil|| d.Decode(&index0) != nil {
		DebugF(dError, "S%d had a error from recover state", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log.entry = entry
		rf.log.index0 = index0
	}
	
	//读取存储中的快照
	rf.snapshot = rf.persister.ReadSnapshot()
	//会不会遗漏了一些机制，比如说snapshotIndex 以及snapshotTerm的设置？？？
	//?R
	rf.snapshotIndex = rf.log.index0
	rf.snapshotTerm = rf.log.entry[0].Term


}


//更新任期的函数
func (rf *Raft) updateCurrentTermL(newTerm int) {
	DPrintf("%v: newTerm %v follower\n", rf.me, newTerm)
    	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
    		return -1, -1, false
	}

	//创造条目并存储到本地日志上
	entry := logEntry{Command: command, Term: rf.currentTerm}
	rf.log.append(entry)
	rf.persist()

	index := rf.log.lastIndex()
	DebugF(dLog, "S%d: get a new entry:%v", rf.me, command)

	//可以尝试不是每个Start都发送一次AppendEntries，而是一个周期发送一次
	//在这里实现某个优化
	rf.sendAppendsL(false)
	return index, rf.currentTerm, true
}



// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft)tick(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader{
		rf.setElectionTimeL()
		rf.sendAppendsL(true)
	}
	if time.Now().After(rf.electionTime){
		rf.setElectionTimeL()
		rf.startElectionL()
	}
}


func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.tick()
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = 0
	if rf.lastApplied + 1 <= rf.log.start(){
		rf.lastApplied = rf.log.start()
		DebugF(dInfo, "S%d restart, lastApplied:%d", rf.me, rf.lastApplied)
		DebugF(dInfo, "S%d restart with snapshot?:%v", rf.me, rf.snapshot != nil)
		//以下要实现的就是当从崩溃中恢复的时候,
		//能够首先将自己的快照进行一个提交
		//目前应该还需构建rf.waitingTerm和rf.waitingIndex等信息。
		//只构建rf.snapshot必然报错。

		/*if rf.snapshot != nil{
			rf.waitingSnapshot = rf.snapshot
			rf.waitingTerm = rf.snapshotTerm
			rf.waitingIndex = rf.snapshotIndex
		}*/
		//以上
	}
	for !rf.killed(){
		if rf.waitingSnapshot != nil{
			message := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotTerm:  rf.waitingTerm,
				SnapshotIndex: rf.waitingIndex,
                 	
			}
			rf.waitingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- message
			rf.mu.Lock()
			DebugF(dSnap, "S%d send a shot with index:%d", rf.me, rf.waitingIndex)

		}else if rf.lastApplied < rf.commitIndex&&
                rf.lastApplied < rf.log.lastIndex()&&
                rf.lastApplied >= rf.log.start(){

			//temp仅用于测试
			rf.lastApplied += 1
			temp := rf.lastApplied
			message := ApplyMsg{
				CommandValid: true,
				Command: rf.log.getEntry(rf.lastApplied,rf.me, "uu1").Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- message
			DebugF(dSnap, "S%d send a msg with index:%d", rf.me, temp)
			rf.mu.Lock()

		}else{

			DebugF(dCommit, "S%d have nothing to apply", rf.me)
			rf.applyCond.Wait()
			DebugF(dCommit, "S%d after wait", rf.me)
		}
	}

}

func (rf *Raft) sendBatchedAppendsPeriodically() {
    for {
        select {
        case <-time.After(time.Millisecond * 300): // 50 毫秒定时器
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return // 当不是 leader 时退出函数
		}
		rf.sendAppendsL(false)
		rf.mu.Unlock()
        }
    }
}

func (rf *Raft) RaftStateSize()int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
        persister *Persister, applyCh chan ApplyMsg) *Raft {
        rf := &Raft{}
        rf.peers = peers
        rf.persister = persister
        rf.me = me
        
	//Persistent state on all servers:
	rf.currentTerm = 0
	rf.votedFor = -1

	//因为日志索引从1开始，所以log开头用哨兵元素占着
	//M	rf.log = []logEntry{{Term: -1}}
	//?R
	rf.log = mkLogEmpty()

	//Volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = 0

	//Volatile state on leaders:
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++{
		rf.nextIndex[i] = 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i< len(rf.peers); i++{
		rf.matchIndex[i] = 0
	}
	
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.snapshot = nil
	rf.waitingSnapshot = nil

        // initialize from state persisted before a crash
        rf.readPersist(persister.ReadRaftState())

        // start ticker goroutine to start elections
        go rf.ticker()
	go rf.applier()
	//go rf.sendBatchedAppendsPeriodically()
        return rf
}
