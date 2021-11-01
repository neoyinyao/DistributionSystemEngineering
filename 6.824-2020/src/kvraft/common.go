package kvraft

import (
	"fmt"
	"log"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	PUT            = "Put"
	//APPEND         = "Append"
	GET            = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Id int64
	Seq int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Id int64
	Seq int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// For Debug
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("GetArgs Id:%v Seq:%v Key:%v ", args.Id,args.Seq,args.Key,)
}

func (reply *GetReply) String() string {
	return fmt.Sprintf("GetReply Err:%v Value:%v", reply.Err, reply.Value)
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs Id:%v Seq:%v Key:%v Value:%v Op:%v", args.Id,args.Seq,args.Key, args.Value, args.Op,)
}

func (reply *PutAppendReply) String() string {
	return fmt.Sprintf("PutAppendReply Err:%v", reply.Err)
}

func (op *Op) String() string {
	return fmt.Sprintf("Op Class:%v Key:%v Value:%v Id:%v Seq:%v", op.Op, op.Key, op.Value,op.Id,op.Seq)
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("KVServer %v State:%v", kv.me, kv.state)
}
