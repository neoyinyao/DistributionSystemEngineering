package kvraft

import (
	"fmt"
	"log"
)


type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	SerialNum int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	ServerId int
}

type GetArgs struct {
	Key       string
	ClientId  int64
	SerialNum int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
	ServerId int
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func (args *GetArgs) String() string {
	return fmt.Sprintf("ClientId : %v SerialNum : %v Key : %v", args.ClientId, args.SerialNum, args.Key)
}
func (reply *GetReply) String() string{
	return fmt.Sprintf("Err : %v ServerId, : %v Value : %v", reply.Err, reply.ServerId, reply.Value)
}
func (args *PutAppendArgs) String() string{
	return  fmt.Sprintf("ClientId : %v SerialNum : %v Op : %v Key : %v Value : %v", args.ClientId, args.SerialNum, args.Op,args.Key,args.Value)
}
func (reply *PutAppendReply)String()string{
	return fmt.Sprintf("Err : %v ServerId, : %v ", reply.Err, reply.ServerId)
}
func (op Op)String()string{
	return fmt.Sprintf("ClientId : %v SerialNum : %v op : %v Key : %v Value : %v",op.ClientId,op.SerialNum,op.Op,op.Key,op.Value)
}