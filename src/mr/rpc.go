package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Arguments for [AskTask] RPC
type AskTaskArgs struct {
	WorkerId int
}
type AskTaskReply struct {
	Task Task
}

// Arguments for [ReportTask] RPC
type ReportTaskArgs struct {
	WorkerId   int
	TaskType   TaskType
	TaskId     int
	TaskResult bool
}
type ReportTaskReply struct {
	WorkerExit bool
}

// Arguments for [GetReduceCount] RPC
type GetReduceCountArgs struct {
}
type GetReduceCountReply struct {
	ReduceCount int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
