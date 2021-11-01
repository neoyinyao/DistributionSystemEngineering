package kvraft

import (
	"6.824/labs/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int
	seq        int
	id         int64
	// You will have to modify this struct.
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
	ck.id = nrand()
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) updateState(leaderId int) {
	ck.seq++
	ck.lastLeader = leaderId
}
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, Id: ck.id, Seq: ck.seq}
	reply := GetReply{}
	DPrintf("client begin Get %v %v", &args, &reply)
	defer DPrintf("client finish Get %v %v", &args, &reply)
	leaderId := ck.lastLeader
	for {
		for ; leaderId < len(ck.servers); leaderId++ {
			ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == ErrNoKey {
				ck.updateState(leaderId)
				return ""
			}
			if reply.Err == OK {
				ck.updateState(leaderId)
				return reply.Value
			}
		}
		if leaderId == len(ck.servers) {
			leaderId = 0
			//DPrintf("client dont get response one iteration")
		}
	}

	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    ck.id,
		Seq:   ck.seq,
	}
	reply := PutAppendReply{}
	DPrintf("client begin PutAppend %v %v", &args, &reply)
	defer DPrintf("client finish PutAppend %v %v", &args, &reply)
	leaderId := ck.lastLeader
	for {
		for ; leaderId < len(ck.servers); leaderId++ {
			ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				continue
			}
			if reply.Err == OK {
				ck.updateState(leaderId)
				return
			}
		}
		if leaderId == len(ck.servers) {
			leaderId = 0
			//DPrintf("client dont get response one iteration")
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
