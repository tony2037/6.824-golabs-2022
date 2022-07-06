package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

/* [TASK]: This section describes task-related structures
 *
 */
type TaskType int
type TaskStatus int

const (
	MapTask TaskType = iota
	ReduceTask
	OtherTask
	ExitTask // all task are done
)

const (
	Idle TaskStatus = iota
	Running
	Finished
)

type Task struct {
	Type   TaskType
	Name   string
	Status TaskStatus
	Index  int
	File   string
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
	mu           sync.Mutex
	aMapTasks    []Task
	aReduceTasks []Task
	nMapTasks    int
	nReduceTasks int
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
// RPC handler: AskForTask
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskForTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	workerId := args.WorkerId
	scheduled := false
	var task *Task
	if 0 < c.nMapTasks {
		scheduled, task = c.ScheduleIdleTask(c.aMapTasks, workerId)
		if !scheduled {
			fmt.Println("Map tasks are all assigned")
		}
	} else if 0 < c.nReduceTasks {
		scheduled, task = c.ScheduleIdleTask(c.aReduceTasks, workerId)
		if !scheduled {
			fmt.Println("Reduce tasks are all assigned")
		}
	} else {
		task = &Task{Status: Finished, Type: ExitTask}
	}
	reply.Task = *task
	c.mu.Unlock()

	go c.MonitorTask(task)
	return nil
}

//
// ScheduleIdleTask
//
func (c *Coordinator) ScheduleIdleTask(taskList []Task, workerId int) (bool, *Task) {
	var task *Task
	for i := 0; i < len(taskList); i++ {
		if Idle == taskList[i].Status {
			task = &taskList[i]
			task.WorkId = workerId
			return true, task
		}
	}
	task = &Task{Status: Finished, Type: ExitTask}
	return false, task
}

//
// ResetTaskIdle
//
func (c *Coordinator) ResetTaskIdle(task *Task) {
	task.Status = Idle
	task.WorkId = -1
}

//
// MonitorTask
//
func (c *Coordinator) MonitorTask(task *Task) bool {
	if task.Type != MapTask && task.Type != ReduceTask {
		return true
	}

	<-time.After(time.Second * TaskTimeout)
	c.mu.Lock()
	if !(task.Status == Finished) {
		// The worker failed to get the task done
		// Set the status back to Idle, so other workers can be assigned to this
		c.ResetTaskIdle(task)
		return false
	}
	c.mu.Unlock()
	return true
}

//
// RPC handler: AskForTask
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	var counter *int
	if args.TaskType == MapTask {
		task = &c.aMapTasks[args.TaskId]
		counter = &c.nMapTasks
	} else if args.TaskType == ReduceTask {
		task = &c.aMapTasks[args.TaskId]
		counter = &c.nReduceTasks
	} else {
		fmt.Println("Should not hit here")
		reply.WorkerExit = false
		return nil
	}

	if args.WorkerId == task.WorkId && !args.TaskResult {
		c.ResetTaskIdle(task)
	} else if args.WorkerId == task.WorkId && Running == task.Status {
		// Consider a situation that, worker 1 and 2 are all assigned to the same job
		task.Status = Finished
		*counter--
		fmt.Printf("The task[%v][%v] is done\n", task.Name, task.Index)
	} else {
		fmt.Printf("The worker[%v] try to finish the task assigned to %v\n", args.WorkerId, task.WorkId)
	}
	reply.WorkerExit = c.AllTaskDone()
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

// AllTaskDone
func (c *Coordinator) AllTaskDone() bool {
	return (c.nMapTasks == 0 && c.nReduceTasks == 0)
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

	ret = c.AllTaskDone()

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
