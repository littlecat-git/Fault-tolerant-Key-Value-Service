package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
//import "time"
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int64	//客户端ID
	lastSeq int	//用于记录本客户端已执行的最大命令编号。
	leaderID int	//用于记录先前的leader编号，一遍快速发送请求
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.lastSeq = 0
	ck.leaderID = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
    args := GetArgs{
        Key:  key,
        ClientID: ck.clientID,
        Seq: ck.lastSeq,
    }
    for{
	    reply := GetReply{}
	    DebugF(dInfo, "c5 send get to K%d ", ck.leaderID)
	    if ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply); ok{
		    if reply.Err == OK || reply.Err == ErrNoKey{
			    ck.lastSeq += 1
			    return reply.Value
		    }

		    //我觉得有可能是leader被堵在某个地方，不能正常回复Call的调用了
	    }
	    ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
    }

    return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) PutAppend(key string, value string, op string) {
    args := PutAppendArgs{
        Key:   key,
        Value: value,
        Op:    op,
        ClientID: ck.clientID,
        Seq: ck.lastSeq,
    }
    for{
            reply := PutAppendReply{}
	    DebugF(dInfo, "c5 send putAppend to K%d ", ck.leaderID)
            if ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply); ok{
                    if reply.Err == OK{
			    DebugF(dInfo,"C5 完成请求:%v", args)
                            ck.lastSeq += 1
			    return
                    }
            }
	    ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
    }

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

