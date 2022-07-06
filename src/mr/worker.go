package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

const PollingSleepTime = 250

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
// @details
//  * a worker can either do a map task or a reduce task
//  * a worker should ask coordinator to assign a task to do
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply, succ := AskForTask()
		if !succ {
			fmt.Println("Fail to ask coordinator to assign a task")
			break
		}
		if ExitTask == reply.Task.Type {
			fmt.Println("All taks are done, exiting ...")
			break
		}
		switch reply.Task.Type {
		// been assigned to do a map task
		case MapTask:
			succ = MapJob(mapf, reply.Task.File, reply.Task.Index)
		// been assigned to do a reduce task
		case ReduceTask:
			succ = ReduceJob(reducef, reply.Task.File, reply.Task.Index)
		// all tasks are assigned, but not all finished
		// poll again after a short period of waiting, in case any worker fail to get task done
		default:
			fmt.Println("All tasks are assigned, but not all finished")
			succ = false
		}

		wokerExit, ok := ReportTask(reply.Task.Type, reply.Task.Index, succ)
		if wokerExit || !ok {
			break
		}

		time.Sleep(time.Millisecond * PollingSleepTime)
	}
}

func AskForTask() (*AskTaskReply, bool) {
	args := AskTaskArgs{os.Getpid()}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskForTask", &args, &reply)
	if ok {
		fmt.Printf("[%v] Success to AskForTask: Name: %v, File: %v\n", args.WorkerId, reply.Task.Name, reply.Task.File)
	} else {
		fmt.Printf("[%v] Fail to AskForTask\n", args.WorkerId)
	}

	return &reply, ok
}

func MapJob(mapf func(string, string) []KeyValue, filePath string, mapId int) bool {
	return false
}

func ReduceJob(reducef func(string, []string) string, filePath string, mapId int) bool {
	return false
}

func ReportTask(taskType TaskType, taskId int, taskResult bool) (bool, bool) {
	args := ReportTaskArgs{os.Getpid(), taskType, taskId, taskResult}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		fmt.Printf("[%v] Fail to ReportTask\n", taskId)
	}
	return reply.WorkerExit, ok
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
