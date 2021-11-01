package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Master struct {
	// Your definitions here.
	mu                  sync.Mutex
	nMap                int
	nReduce             int
	mapDone             bool
	reduceDone          bool
	mapTaskState        []int
	reduceTaskState     []int
	mapFinishedCount    int
	reduceFinishedCount int
	filenames           []string
}

type Task struct {
	Id       int
	TaskType string
	ReduceNum  int
	FileName string
	MapNum     int
	mapf	func(string, string) []KeyValue
	reducef func(string, []string) string
}

const UNDO = 0
const PROCESSING = 1
const FINISHED = 2

// Your code here -- RPC handlers for the worker to call.
func (m *Master) DistributeTask(Args *EmptyArgs, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.mapDone {
		for Idx, state := range m.mapTaskState {
			if state == UNDO {
				task.Id = Idx
				task.TaskType = "map"
				//task.state = PROCESSING
				task.ReduceNum = m.nReduce
				task.FileName = m.filenames[Idx]
				//log.Printf("%v is distributed",Idx)
				m.mapTaskState[Idx] = PROCESSING
				return nil
			}
		}
		task.TaskType = "waiting"
		return nil
	}
	if !m.reduceDone {
		for Idx, state := range m.reduceTaskState {
			if state == UNDO {
				task.Id = Idx
				task.TaskType = "reduce"
				task.MapNum = m.nMap
				m.reduceTaskState[Idx] = PROCESSING
				return nil
			}
		}
		task.TaskType = "waiting"
		return nil
	}
	task.TaskType = "finished"
	return nil
}

func (m *Master) UpdateTaskState(args *UpdateArgs, Reply *EmptyRely) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.IsSuccess {
		if args.TaskType == "map" {
			m.mapTaskState[args.Id] = FINISHED
			m.mapFinishedCount++
		} else {
			m.reduceTaskState[args.Id] = FINISHED
			m.reduceFinishedCount++
		}
	} else {
		if args.TaskType == "map" {
			m.mapTaskState[args.Id] = UNDO
		} else {
			m.mapTaskState[args.Id] = UNDO
		}
	}
	if m.mapFinishedCount == m.nMap {
		m.mapDone = true
	}
	if m.reduceFinishedCount == m.nReduce {
		m.reduceDone = true
	}
	return nil
}
func (m *Master) HandleTimeOut() {
	for {
		time.Sleep(time.Second * 10)
		m.mu.Lock()
		if m.mapDone && m.reduceDone{
			m.mu.Unlock()
			break
		}
		if !m.mapDone {
			for Idx,state := range m.mapTaskState{
				if state == PROCESSING{
					m.mapTaskState[Idx] = UNDO
				}
			}
			m.mu.Unlock()
		}else if !m.reduceDone{
			for Idx,state := range m.reduceTaskState{
				if state == PROCESSING{
					m.reduceTaskState[Idx] = UNDO
				}
			}
			m.mu.Unlock()
		}

	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapDone && m.reduceDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	//初始化master
	m := Master{
		mu:                  sync.Mutex{},
		nMap:                len(files),
		nReduce:             nReduce,
		mapDone:             false,
		reduceDone:          false,
		mapTaskState:        make([]int, len(files)),
		reduceTaskState:     make([]int, nReduce),
		mapFinishedCount:    0,
		reduceFinishedCount: 0,
		filenames:           files,
	}
	// Your code here.

	m.server()
	go m.HandleTimeOut()
	return &m
}
