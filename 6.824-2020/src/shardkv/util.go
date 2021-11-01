package shardkv

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func (args *GetArgs) String() string {
	return fmt.Sprintf("GetArgs ClientId: %v,Seq: %v, ConfigNum: %v,Shard: %v,Key:%v", args.ClientId, args.Seq, args.ConfigNum, args.Shard, args.Key)
}
func (args *GetReply) String() string {
	return fmt.Sprintf("GetReply Err:  %v,Value: %v", args.Err, args.Value)
}
func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs ClientId: %v,Seq: %v, ConfigNum: %v,Shard: %v,Key:%v,Value:%v", args.ClientId, args.Seq, args.ConfigNum, args.Shard, args.Key,args.Value)
}
func (args *PutAppendReply) String() string {
	return fmt.Sprintf("PutAppendReply Err: %v", args.Err)
}
func (op *Op) String() string {
	return fmt.Sprintf("Op ClientId: %v,Seq: %v,Type: %v", op.ClientId,op.Seq,op.Type)
}
func (kv *ShardKV) String() string{
	return fmt.Sprintf("ShardKV [%v][%v],configNum: %v shardConfigNum %v", kv.gid,kv.me,kv.newConfig.Num,kv.shardConfigNum)
}

//func (sm *ShardMaster) String() string{
//	return fmt.Sprintf("QueryArgs Id  : %v,Seq :  %v, Num  : %v ", sm.me,sm.)
//}
