package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var Lock sync.Mutex

//Task元数据
type TaskMetaData struct {
	State      TaskState //Task当前运行状态
	TimeIssued time.Time
	Task       *Task
}
type TaskMetaDataHolder []*TaskMetaData

type Coordinator struct {
	// Your definitions here.
	MapNum                   int
	ReduceNum                int
	TaskPhase                TaskPhase
	Files                    []string
	TaskChanMap              chan *Task
	TaskChanReduce           chan *Task
	MapTaskMetaDataHolder    TaskMetaDataHolder
	ReduceTaskMetaDataHolder TaskMetaDataHolder
}

// Your code here -- RPC handlers for the worker to call.

//主节点处理工作节点报告完成任务RPC请求
func (c *Coordinator) HandleFinishTask(args *CallDoneArgs, reply *CallDoneReply) error {
	Lock.Lock()
	defer Lock.Unlock()
	if args.TaskType == MapTask {
		c.MapTaskMetaDataHolder[args.TaskId].State = CompleteState
		//fmt.Printf("MapTask: %v has done\n", args.TaskId)
	} else {
		c.ReduceTaskMetaDataHolder[args.TaskId].State = CompleteState
		//fmt.Printf("ReduceTask: %v has done\n", args.TaskId)
	}
	return nil
}
func (c *Coordinator) checkTaskAllDone() bool {
	flag := true
	if c.TaskPhase == MapPhase {
		for i := 0; i < c.MapNum; i++ {
			if c.MapTaskMetaDataHolder[i].State != CompleteState {
				flag = false
			}
		}
	} else if c.TaskPhase == ReducePhase {
		for i := 0; i < c.ReduceNum; i++ {
			if c.ReduceTaskMetaDataHolder[i].State != CompleteState {
				flag = false
			}
		}
	}
	return flag
}
func (c *Coordinator) changePhase() {
	if c.TaskPhase == MapPhase {
		c.TaskPhase = ReducePhase
	} else if c.TaskPhase == ReducePhase {
		c.TaskPhase = AllDonePhase
	}
}

//主节点处理分配任务RPC请求
func (c *Coordinator) HandleDistributeTask(args *CallDistributeArgs, reply *CallDistributeReply) error {
	Lock.Lock()
	defer Lock.Unlock()

	switch c.TaskPhase {
	case MapPhase:
		if len(c.TaskChanMap) > 0 {
			task := <-c.TaskChanMap
			c.MapTaskMetaDataHolder[task.TaskId].State = WorkingState
			c.MapTaskMetaDataHolder[task.TaskId].TimeIssued = time.Now()
			reply.Task = task
		} else {
			reply.Task = &Task{
				TaskType: WaittingTask,
			}
			if c.checkTaskAllDone() {
				c.changePhase()
			}
		}
	case ReducePhase:
		if len(c.TaskChanReduce) > 0 {
			task := <-c.TaskChanReduce
			c.ReduceTaskMetaDataHolder[task.TaskId].State = WorkingState
			c.ReduceTaskMetaDataHolder[task.TaskId].TimeIssued = time.Now()
			reply.Task = task
		} else {
			reply.Task = &Task{
				TaskType: WaittingTask,
			}
			if c.checkTaskAllDone() {
				c.changePhase()
			}
		}
	case AllDonePhase:
		reply.Task = &Task{
			TaskType: ExitTask,
		}
	}
	return nil
}
func (c *Coordinator) checkTimeOut() {
	flag := true
	for flag {
		time.Sleep(time.Second * 1)
		Lock.Lock()
		if c.TaskPhase == MapPhase {
			for i := 0; i < c.MapNum; i++ {
				if c.MapTaskMetaDataHolder[i].State == WorkingState &&
					time.Since(c.MapTaskMetaDataHolder[i].TimeIssued) > 30*time.Second {
					//fmt.Printf("map task %v time out\n", c.MapTaskMetaDataHolder[i].Task.TaskId)
					c.MapTaskMetaDataHolder[i].State = WaittingState
					c.TaskChanMap <- c.MapTaskMetaDataHolder[i].Task
				}
			}
		} else if c.TaskPhase == ReducePhase {
			for i := 0; i < c.ReduceNum; i++ {
				if c.ReduceTaskMetaDataHolder[i].State == WorkingState &&
					time.Since(c.ReduceTaskMetaDataHolder[i].TimeIssued) > 30*time.Second {
					//fmt.Printf("reduce task %v time out\n", c.ReduceTaskMetaDataHolder[i].Task.TaskId)
					c.ReduceTaskMetaDataHolder[i].State = WaittingState
					c.TaskChanReduce <- c.ReduceTaskMetaDataHolder[i].Task
				}
			}
		} else {
			flag = false
		}
		Lock.Unlock()
	}
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
	ret := false

	// Your code here.
	Lock.Lock()
	defer Lock.Unlock()
	if c.TaskPhase == AllDonePhase {
		ret = true
		time.Sleep(time.Second * 5)
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapNum:                   len(files),
		ReduceNum:                nReduce,
		TaskPhase:                MapPhase,
		Files:                    files,
		TaskChanMap:              make(chan *Task, len(files)),
		TaskChanReduce:           make(chan *Task, nReduce),
		MapTaskMetaDataHolder:    make(TaskMetaDataHolder, len(files)),
		ReduceTaskMetaDataHolder: make(TaskMetaDataHolder, nReduce),
	}

	c.MakeTaskMetaData()
	time.Sleep(time.Second)

	go c.checkTimeOut()

	c.server()
	return &c
}

func (c *Coordinator) MakeTaskMetaData() {
	for i, file := range c.Files {
		c.AcceptTaskMetaData(i, file, MapTask)
		c.TaskChanMap <- c.MapTaskMetaDataHolder[i].Task
	}
	for i := 0; i < c.ReduceNum; i++ {
		c.AcceptTaskMetaData(i, "", ReduceTask)
		c.TaskChanReduce <- c.ReduceTaskMetaDataHolder[i].Task
	}
}

func (c *Coordinator) AcceptTaskMetaData(TaskId int, FileName string, taskType TaskType) {
	tmpTask := TaskMetaData{
		State: WaittingState,
		Task: &Task{
			TaskType:  taskType,
			FileName:  FileName,
			TaskId:    TaskId,
			ReduceNum: c.ReduceNum,
			MapNum:    c.MapNum,
		},
	}
	if taskType == MapTask {
		c.MapTaskMetaDataHolder[TaskId] = &tmpTask
	} else {
		c.ReduceTaskMetaDataHolder[TaskId] = &tmpTask
	}
}
