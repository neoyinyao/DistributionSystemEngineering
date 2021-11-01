package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type taskState struct {
	state     string
	startTime time.Time
}
type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	files           []string
	nMap            int
	nReduce         int
	mapTaskState    []*taskState
	mapFinish       bool
	reduceTaskState []*taskState
	reduceFinish    bool
	judgeDelay      time.Duration
}

func (c *Coordinator) ScheduleTask(args *TaskScheduleArgs, reply *TaskScheduleReply) error{
	c.mu.Lock()
	DPrintf("coordinator : ",c)
	defer c.mu.Unlock()
	switch {
	case !c.mapFinish:
		for index, state := range c.mapTaskState {
			if state.state == "idle" {
				reply.Class = "map"
				reply.Filename = c.files[index]
				reply.NReduce = c.nReduce
				reply.Index = index + 1
				c.mapTaskState[index].state = "process"
				c.mapTaskState[index].startTime = time.Now()
				return nil
			}
		}
	case !c.reduceFinish:
		for index, state := range c.reduceTaskState {
			if state.state == "idle" {
				reply.Class = "reduce"
				reply.Index = index + 1
				reply.NMap = c.nMap
				c.reduceTaskState[index].state = "process"
				c.reduceTaskState[index].startTime = time.Now()
				return nil
			}
		}
	default:
	}
	return nil
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, reply *TaskFinishReply) error{
	c.mu.Lock()
	DPrintf("taskFinish args :",args)
	defer c.mu.Unlock()
	if args.Class == "map" {
		c.mapTaskState[args.Index-1].state = "finish"
		return nil
	} else {
		c.reduceTaskState[args.Index-1].state = "finish"
		return nil
	}
}
func (c *Coordinator) MonitorState() {
	for {
		c.mu.Lock()
		isMapFinish := true
		if !c.mapFinish {
			for _, state := range c.mapTaskState {
				if state.state == "idle" || state.state == "process"{
					isMapFinish = false
				}
				if state.state == "process" && (time.Now().Sub(state.startTime) > c.judgeDelay) {
					state.state = "idle"
					isMapFinish = false
				}
			}
		}
		isReduceFinish := true
		if !c.reduceFinish {
			for _, state := range c.reduceTaskState {
				if state.state == "idle" || state.state == "process"{
					isReduceFinish = false
				}
				if state.state == "process" && (time.Now().Sub(state.startTime) > c.judgeDelay) {
					state.state = "idle"
					isReduceFinish = false
				}
			}
		}
		c.mapFinish = isMapFinish
		c.reduceFinish = isReduceFinish
		c.mu.Unlock()
		if c.reduceFinish {
			fmt.Sprintf("finish job",c.files)
			return
		}
		time.Sleep(time.Second * 10)
	}

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceFinish
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	DPrintf("inputFiles:",files)
	c := Coordinator{
		mu:              sync.Mutex{},
		files:           files,
		nMap:            len(files),
		nReduce:         nReduce,
		mapTaskState:    make([]*taskState, len(files)),
		mapFinish:       false,
		reduceTaskState: make([]*taskState, nReduce),
		reduceFinish:    false,
		judgeDelay:      time.Second * 10,
	}
	for i := 0;i<len(files);i++{
		c.mapTaskState[i] = &taskState{
			state:     "idle",
			startTime: time.Time{},
		}
	}
	for i:=0;i<nReduce;i++{
		c.reduceTaskState[i] = &taskState{
			state:     "idle",
			startTime: time.Time{},
		}
	}
	go c.MonitorState()
	// Your code here.

	c.server()
	return &c
}
