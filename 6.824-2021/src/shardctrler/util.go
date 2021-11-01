package shardctrler

import (
	"fmt"
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (config *Config) String() string{
	return fmt.Sprintf("Num : %v,Shards : %v,Groups : %v",config.Num,config.Shards,config.Groups)
}
func (op *Op) String() string{
	return fmt.Sprintf("Id : %v,SerialNum : %v,Op : %v,Servers : %v,GIDs : %v,Shard : %v,GID : %v,Num : %v",op.Id,op.SerialNum,op.Op,op.Servers,op.GIDs,op.Shard,op.GID,op.Num)
}