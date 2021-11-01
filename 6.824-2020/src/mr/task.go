package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func (task Task) doMap(){
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		task.callUpdateTask(false)
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		task.callUpdateTask(false)
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := task.mapf(filename, string(content))

	intermediate := make([][]KeyValue, task.ReduceNum, task.ReduceNum)
	// 分区
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.ReduceNum
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for idx := 0; idx < task.ReduceNum; idx++ {
		// 之所以idx到ReduceNum为止，是因为需要生成ReduceNum个中间文件提供给Reduce任务处理
		intermediateFileName := fmt.Sprintf("mr-%d-%d", task.Id, idx)
		// 将内容写入到intermediateFile
		file, err := ioutil.TempFile("","temp")
		if err != nil {
			task.callUpdateTask(false)
			log.Fatalf("cannot create %v", file.Name())
		}
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		if err != nil {
			task.callUpdateTask(false)
			log.Fatalf("cannot write %v", file.Name())
		}
		file.Close()
		os.Rename(file.Name(),intermediateFileName)
	}
	//log.Printf("%v map is finished",task.Id)
	// 向master报告
	task.callUpdateTask(true)

}
func (task Task) doReduce(){
	kvsReduce := make(map[string][]string)
	for idx := 0; idx < task.MapNum; idx++ {
		// 之所以idx到MapNum为止，是因为只有这么多个文件
		intermediateFileName := fmt.Sprintf("mr-%d-%d", idx, task.Id)
		content, err := ioutil.ReadFile(intermediateFileName)
		if err != nil {
			task.callUpdateTask(false)
			log.Fatalf("read file %v failed", intermediateFileName)
			return
		}
		kvs := make([]KeyValue, 0)
		// 把对应文件中的key-value pair读入到kvs中
		err = json.Unmarshal(content, &kvs)
		if err != nil {
			task.callUpdateTask(false)
			log.Fatalf("fail to unmarshal the data")
			return
		}
		// 将每个单词出现的记录聚合起来，准备统计
		for _, kv := range kvs {
			if _, ok := kvsReduce[kv.Key]; !ok {
				kvsReduce[kv.Key] = make([]string, 0)
			}
			kvsReduce[kv.Key] = append(kvsReduce[kv.Key], kv.Value)
		}
	}
	result := make([]string, 0)
	for key, val := range kvsReduce {
		output := task.reducef(key, val)
		result = append(result, fmt.Sprintf("%v %v\n", key, output))
	}
	outputFileName := fmt.Sprintf("mr-out-%d", task.Id)
	err := ioutil.WriteFile(outputFileName, []byte(strings.Join(result, "")), 0644)
	if err != nil {
		task.callUpdateTask(false)
		log.Fatalf("write result failed")
		return
	}
	//log.Printf("%v reduce is finished",task.Id)
	task.callUpdateTask(true)
}
