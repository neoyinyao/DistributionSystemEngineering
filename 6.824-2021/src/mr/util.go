package mr

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (reply *TaskScheduleReply) String() string{
	return fmt.Sprintf("index : %v nMap : %v nReduce : %v filename : %v class : %v", reply.Index, reply.NMap, reply.NReduce, reply.Filename, reply.Class)
}
func (c *Coordinator) String() string{
	return fmt.Sprintf("mapFinish:%v reduceFinish:%v",c.mapFinish,c.reduceFinish)
}
func (args *TaskFinishArgs) String() string{
	return fmt.Sprintf("class : %v index : %v",args.Class,args.Index)
}