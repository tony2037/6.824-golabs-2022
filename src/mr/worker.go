package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
	"io/ioutil"
	"bufio"
	"encoding/json"
	"path/filepath"
	"sort"
)

const PollingSleepTime = 250

var nReduce int

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
	n, succ := GetReduceCount()
	if succ == false {
		fmt.Println("Failed to get reduce task count, worker exiting.")
		return
	}
	nReduce = n

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
			succ = ReduceJob(reducef, reply.Task.Index)
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
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
		return false
	}
	
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err)
		return false
	}
	file.Close()

	kva := mapf(filePath, string(content))
	WriteMapOutput(kva, mapId)
	return true
}

func WriteMapOutput(kva []KeyValue, mapId int) {
	// Adopt from others
	// use io buffers to reduce disk I/O
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	// temp files, use pid to uniquely identify this worker
	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatal(err)
		}
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	// write to tmp files
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	// flush file buffer to disk
	for i, buf := range buffers {
		err := buf.Flush()
		if err != nil {
			log.Fatalf("the buffer[%d] has problem: %v", i, err)
		}
	}

	// rename tmp files to ensure no observes partial files
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("[%v] Fail to rename file %v", err, file.Name())
		}
	}
}

func ReduceJob(reducef func(string, []string) string, reduceId int) bool {
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		log.Fatalf("[%v] Cannot glob reduce files", err)
		return false
	}
	kvMap := make(map[string][]string)
	var kv KeyValue
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("[%v] Fail to open file %v", err, filePath)
			return false
		}

		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatal(err)
				return false
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	WriteReduceOutput(reducef, kvMap, reduceId)
	return true
}

func WriteReduceOutput(reducef func(string, []string) string, kvMap map[string][]string, reduceId int) {
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}

	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
		if err != nil {
			log.Fatalf("[%v] Connot write mr output [%v, %v]", err, k, v)
		}
	}
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	if err != nil {
		log.Fatalf("[%v] Fail to rename %v to %v", err, filePath, newPath)
	}
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

func GetReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Coordinator.GetReduceCount", &args, &reply)

	return reply.ReduceCount, succ
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
