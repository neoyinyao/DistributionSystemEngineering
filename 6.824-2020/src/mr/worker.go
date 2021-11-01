package mr

import (
	"fmt"
	"log"
	"time"
)
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	task := Task{mapf: mapf, reducef: reducef}
	for {
		task.callDistributeTask()
		//log.Printf("%v ", task)
		switch task.TaskType {
		case "map":
			//log.Printf("%v map is start execute", task.Id)
			task.doMap()
		case "reduce":
			//log.Printf("%v reduce is start execute", task.Id)
			task.doReduce()
		case "waiting":
			time.Sleep(time.Second)
		case "finished":
			break
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}
func (task *Task) callDistributeTask() {
	args := EmptyArgs{}
	newTask := Task{}
	call("Master.DistributeTask", &args, &newTask)
	//log.Printf("task: %v\n", newTask)
	task.TaskType = newTask.TaskType
	task.Id = newTask.Id
	task.FileName = newTask.FileName
	task.MapNum = newTask.MapNum
	task.ReduceNum = newTask.ReduceNum
}
func (task *Task) callUpdateTask(IsSuccess bool) {
	args := UpdateArgs{
		Id:        task.Id,
		TaskType:  task.TaskType,
		IsSuccess: IsSuccess,
	}
	reply := EmptyRely{}
	call("Master.UpdateTaskState", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
