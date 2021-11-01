package shardmaster
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
func (args *JoinArgs) String() string {
	return fmt.Sprintf("JoinArgs Id : %v,Seq  : %v, Servers  : %v", args.Id, args.Seq, args.Servers)
}
func (args *LeaveArgs) String() string {
	return fmt.Sprintf("LeaveArgs Id :  %v,Seq  : %v, GIDs  : %v", args.Id, args.Seq, args.GIDs)
}
func (args *MoveArgs) String() string {
	return fmt.Sprintf("MoveArgs Id  : %v,Seq :  %v, Shard  : %v GID  : %v", args.Id, args.Seq, args.Shard,args.GID)
}
func (args *QueryArgs) String() string{
	return fmt.Sprintf("QueryArgs Id  : %v,Seq :  %v, Num  : %v ", args.Id, args.Seq, args.Num)
}
func (op *Op) String() string{
	return fmt.Sprintf("op Id  : %v,Seq :  %v", op.Id, op.Seq)
}

//func (sm *ShardMaster) String() string{
//	return fmt.Sprintf("QueryArgs Id  : %v,Seq :  %v, Num  : %v ", sm.me,sm.)
//}

