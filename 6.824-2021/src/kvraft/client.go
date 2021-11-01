package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id         int64
	serialNum  int
	lastLeader int
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
	ck.serialNum = 1
	ck.lastLeader = 0
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
func (ck *Clerk) Get(key string) string {
	i := ck.lastLeader
	for {
		for ; i < len(ck.servers); i++ {
			args := GetArgs{
				Key:       key,
				ClientId:  ck.id,
				SerialNum: ck.serialNum,
			}
			reply := GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == NOTLEADER {
					continue
				} else if reply.Err == OK{
					ck.lastLeader = reply.ServerId
					ck.serialNum++
					return reply.Value
				}else {
					panic("reply.Err neither OK nor NOTLEADER,cannot happen")
				}
			}
		}
		i = 0
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
	i := ck.lastLeader
	for {
		for ; i < len(ck.servers); i++ {
			args := PutAppendArgs{
				Key:       key,
				Value:     value,
				Op:        op,
				ClientId:  ck.id,
				SerialNum: ck.serialNum,
			}
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == NOTLEADER {
					continue
				} else if reply.Err == OK{
					ck.lastLeader = reply.ServerId
					ck.serialNum++
					return
				}else {
					panic("reply.Err neither OK nor NOTLEADER,cannot happen")
				}
			}
		}
		i = 0
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
