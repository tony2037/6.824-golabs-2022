package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "path/filepath"

/* [TASK]: This section describes task-related structures
 *
*/
type TaskType int
type TaskStatus int

const (
	MapTask TaskType = iota
	ReduceTask
	OtherTask
)

const (
	Idle TaskStatus = iota
	Running
	Finished
)

type Task struct {
	Type TaskType
	Name string
	Status TaskStatus
	Index int
	File string
	WorkId int
}

/* [TASK]: end */

/* [Configureation]
*/
const TempDir = "tmp"
const ResultFiles = "mr-out*"
const TaskTimeout = 10
/**/

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	aMapTasks       []Task
	aReduceTasks    []Task
	nMapTasks       int
	nReduceTasks    int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

	ret = ( 0 == c.nMapTasks && 0 == c.nReduceTasks)

	return ret
}

//
// Clean up the environment
//
//
func Destroy() {
	outFiles, _ := filepath.Glob(ResultFiles)
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Fail to remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Fail to remove directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Fail to create directory %v\n", TempDir)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMapTasks := len(files)
	c.nMapTasks = nMapTasks
	c.aMapTasks = make([]Task, 0, nMapTasks)
	c.nReduceTasks = nReduce
	c.aReduceTasks = make([]Task, 0, nReduce)

	for i := 0; i < nMapTasks; i++ {
		tTaskName := fmt.Sprintf("[MAP](%d)", i)
		tTask := Task{MapTask, tTaskName, Idle, i, files[i], -1}
		c.aMapTasks = append(c.aMapTasks, tTask)
	}
	for i := 0; i < nReduce; i++ {
		tTaskName := fmt.Sprintf("[REDUCE](%d)", i)
		tTask := Task{MapTask, tTaskName, Idle, i, "", -1}
		c.aReduceTasks = append(c.aReduceTasks, tTask)
	}

	c.server()

	Destroy()
	return &c
}
