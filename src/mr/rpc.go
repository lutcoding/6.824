package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Task struct {
	TaskType  TaskType
	FileName  string
	TaskId    int
	ReduceNum int
	MapNum    int
}

// 定义任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask //当前没有任务可以分发
	ExitTask     //所有任务完成
)

// 定义任务阶段
type TaskPhase int

const (
	MapPhase     TaskPhase = iota //当前处于Map阶段
	ReducePhase                   //当前处于Reduce阶段
	AllDonePhase                  //当前所有任务都完成
)

// 定义任务的当前状态
type TaskState int

const (
	WaittingState TaskState = iota //等待执行
	WorkingState                   //正在执行
	CompleteState                  //执行完成
)

type CallDistributeArgs struct {
}

type CallDistributeReply struct {
	Task *Task
}

type CallDoneArgs struct {
	TaskId   int
	TaskType TaskType
}

type CallDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
