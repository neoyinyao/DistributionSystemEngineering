package mr

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func executeMapTask(mapf func(string, string) []KeyValue, filename string, nReduce int,indexMap int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	sort.Sort(ByKey(intermediate))
	fmt.Printf("symbol 1 of %v",indexMap)
	//shuffle
	reduceKvs := make([][]KeyValue,nReduce)
	for i,_ := range reduceKvs{
		reduceKvs[i] = []KeyValue{}
	}
	for _,kv := range intermediate{
		index := ihash(kv.Key) % nReduce
		reduceKvs[index] = append(
			reduceKvs[index],
			kv,
		)
	}
	fmt.Printf("symbol 2 of %v",indexMap)
	for i:=0;i<nReduce;i++{
		tmpfile, err := ioutil.TempFile("", "example")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range reduceKvs[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		mapOutputFile := "map-"+strconv.Itoa(indexMap) + "-" + strconv.Itoa(i+1)
		os.Rename(tmpfile.Name(),mapOutputFile)
	}
	fmt.Printf("map %v is finished",indexMap)
	args := TaskFinishArgs{
		Class: "map",
		Index: indexMap,
	}
	reply := TaskFinishReply{}
	go call("Coordinator.TaskFinish", &args, &reply)
}
func executeReduceTask(reducef func(string, []string) string, nMap int, indexReduce int) {
	var intermediate []KeyValue
	for i:=1;i<nMap+1;i++{
		filename := "map-"+strconv.Itoa(i)+"-"+strconv.Itoa(indexReduce)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"+strconv.Itoa(indexReduce)

	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		log.Fatal(err)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpfile.Close()
	os.Rename(tmpfile.Name(),oname)
	args := TaskFinishArgs{
		Class: "reduce",
		Index:indexReduce,
	}
	reply := TaskFinishReply{}
	go call("Coordinator.TaskFinish", &args, &reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := TaskScheduleArgs{}
		reply := TaskScheduleReply{}
		isSucceed := call("Coordinator.ScheduleTask", &args, &reply)
		fmt.Println(isSucceed)
		if !isSucceed {
			return
		}
		DPrintf("scheduleReply :",&reply)
		switch reply.Class {
		case "map":
			executeMapTask(mapf, reply.Filename, reply.NReduce,reply.Index)
		case "reduce":
			executeReduceTask(reducef, reply.NMap, reply.Index)
		default:
			time.Sleep(time.Second)
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
