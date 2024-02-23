package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

func GetIntermediateFileName(MapId, ReduceId int) string {
	return fmt.Sprintf("mr-%v-%v", MapId, ReduceId)
}
func FinalizeIntermediateFile(FileName string, MapId, ReduceId int) {
	name := GetIntermediateFileName(MapId, ReduceId)
	os.Rename(FileName, "./"+name)
}
func FinalizeReduceFile(fileName string, taskId int) {
	file := fmt.Sprintf("mr-out-%v", taskId)
	os.Rename(fileName, "./"+file)
}
func ProcessMapTask(task *Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()

	kva := mapf(task.FileName, string(content))

	tmpFiles := []*os.File{}
	tmpFileNames := []string{}
	encoders := []*json.Encoder{}

	for i := 0; i < task.ReduceNum; i++ {
		tempFile, err := ioutil.TempFile("./", "")
		if err != nil {
			log.Printf("Can not open tmpFile")
		}
		tmpFiles = append(tmpFiles, tempFile)
		tmpFileNames = append(tmpFileNames, tempFile.Name())
		encoders = append(encoders, json.NewEncoder(tempFile))
	}
	for _, kv := range kva {
		encoders[ihash(kv.Key)%task.ReduceNum].Encode(kv)
	}
	for _, tmpFile := range tmpFiles {
		tmpFile.Close()
	}
	for i := 0; i < task.ReduceNum; i++ {
		FinalizeIntermediateFile(tmpFileNames[i], task.TaskId, i)
	}
}

func ProcessReduceTask(task *Task, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < task.MapNum; i++ {
		fileName := GetIntermediateFileName(i, task.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		decoder := json.NewDecoder(file)
		for decoder.More() {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	file, err := ioutil.TempFile("./", "")
	tmpFileName := file.Name()
	if err != nil {
		log.Fatalf("can not open tempFile")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	file.Close()
	FinalizeReduceFile(tmpFileName, task.TaskId)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := CallTask()
		if task == nil {
			time.Sleep(time.Second * 2)
			continue
		}
		switch task.TaskType {
		case MapTask:
			//now := time.Now()
			ProcessMapTask(task, mapf)
			//fmt.Printf("spend %v to finish task:%v\n", time.Since(now), task.TaskId)
			CallDone(task)
		case ReduceTask:
			ProcessReduceTask(task, reducef)
			CallDone(task)
			//另一种方式可以是主节点一直等待有任务可以分发时再发送，如果没有任务，则worker的call将会阻塞
			//如果使用WaittingTask，worker将会不断发送rpc请求，有可能增加网络流量
			//两种方法都可行
		case WaittingTask:
			//fmt.Println("need waitting")
		case ExitTask:
			//fmt.Println("all task done, worker exit")
			os.Exit(0)
		}
		time.Sleep(time.Second * 2)
	}
}

//
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallTask() *Task {

	args := CallDistributeArgs{}

	reply := CallDistributeReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.HandleDistributeTask", &args, &reply)
	if ok {
		return reply.Task
	} else {
		return nil
	}
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

func CallDone(task *Task) {
	args := CallDoneArgs{
		TaskId:   task.TaskId,
		TaskType: task.TaskType,
	}

	reply := CallDoneReply{}
	i := 0
	for ok := call("Coordinator.HandleFinishTask", &args, &reply); !ok; {
		i++
		if i > 5 {
			break
		}
	}
}
