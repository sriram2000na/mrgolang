package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskType string
type TaskState string

const MAP_TASK_TYPE TaskType = "MAP"
const REDUCE_TASK_TYPE TaskType = "REDUCE"
const TASK_STATE_INIT TaskState = "INIT"
const TASK_STATE_ASSIGNED TaskState = "ASSIGNED"
const TASK_STATE_DONE TaskState = "DONE"

var task_id = 0

type Task struct {
	Name         TaskType
	Arg_key      string
	Arg_values   []string
	Arg_filename string
	Issued_at    *time.Time
	CurrentState TaskState
	TaskId       int
}

type Coordinator struct {
	// Your definitions here.
	files     []string
	map_tasks []*Task
	red_tasks []*Task
	remain    int
	nreduces  int
}

var map_tasks_mu sync.Mutex
var red_tasks_mu sync.Mutex

var worker_id atomic.Int32
var done = false

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetR(_ *struct{}, out *int) error {
	// fmt.Printf("Sending nreduces: %d\n", c.nreduces)
	*out = c.nreduces
	return nil
}

func (c *Coordinator) GetId(_ *struct{}, out *int) error {
	current_id := worker_id.Add(1)
	*out = int(current_id)
	// fmt.Printf("Sending worker id: %d\n", *out)
	// fmt.Printf("Printing current_id: %d\n", current_id)
	return nil
}

func taskTimedOut(t Task) bool {
	if t.Issued_at == nil {
		return false
	}
	return t.CurrentState == TASK_STATE_ASSIGNED && time.Now().After(t.Issued_at.Add(10*time.Second))
}

func selectTask(c *Coordinator, reply *GetTaskReply, taskType TaskType) {
	var taskList []*Task
	if taskType == MAP_TASK_TYPE {
		taskList = c.map_tasks
	} else {
		taskList = c.red_tasks
	}
	var outTask *Task = nil
	for i, task := range taskList {
		can_issue := (task.CurrentState == TASK_STATE_INIT && task.Issued_at == nil) || taskTimedOut(*task)
		if can_issue {
			reply.HasTask = true
			outTask = taskList[i]
			break
		}
	}
	if outTask == nil {
		return
	}
	outTask.CurrentState = TASK_STATE_ASSIGNED
	current_time := time.Now()
	(*outTask).Issued_at = &current_time
	// fmt.Printf("Picked task: %d\n", outTask.TaskId)
	reply.Task = *outTask
}

func (c *Coordinator) GetTask(_ *struct{}, reply *GetTaskReply) error {
	reply.HasTask = false

	map_tasks_mu.Lock()
	red_tasks_mu.Lock()
	defer map_tasks_mu.Unlock()
	defer red_tasks_mu.Unlock()
	if len(c.map_tasks) == 0 && len(c.red_tasks) == 0 {
		return nil
	}
	done_map_tasks := 0
	for _, t := range c.map_tasks {
		if t.CurrentState == TASK_STATE_DONE {
			done_map_tasks++
		}
	}
	if len(c.map_tasks) > done_map_tasks {
		selectTask(c, reply, MAP_TASK_TYPE)
		// fmt.Println("Selected map task")
	} else {
		selectTask(c, reply, REDUCE_TASK_TYPE)
		// fmt.Println("Selected red task")
	}
	return nil
}

var ctr = 0

func (c *Coordinator) TaskOverNotify(task Task, response *bool) error {
	// fmt.Println("Got task over call")
	defer func() {
		*response = done
	}()
	// fmt.Printf("TaskOver: %+v", task)
	if task.Name == MAP_TASK_TYPE {
		map_tasks_mu.Lock()
		ctr = 0
		for i, val := range c.map_tasks {
			// fmt.Printf("TaskOver task id: %d\n", task.TaskId)
			if val.TaskId == task.TaskId {
				// fmt.Println("Task over: inside if:")
				c.map_tasks[i].CurrentState = TASK_STATE_DONE
				// fmt.Println(c.map_tasks[i].CurrentState == TASK_STATE_DONE)
			}
			if c.map_tasks[i].CurrentState == TASK_STATE_DONE {
				ctr++
			}
		}
		map_tasks_mu.Unlock()
		// fmt.Printf("The total number of map tasks completed: %d, out of %d tasks\n", ctr, len(c.map_tasks))
		return nil
	} else {
		totallyComplete := true
		red_tasks_mu.Lock()
		for i, val := range c.red_tasks {
			if val.TaskId == task.TaskId {
				c.red_tasks[i].CurrentState = TASK_STATE_DONE
			}
			totallyComplete = totallyComplete && c.red_tasks[i].CurrentState == TASK_STATE_DONE
		}
		if totallyComplete {
			//TODO: make rpc calls to stop the workers
			done = true
		}
		red_tasks_mu.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	fmt.Println("Coordinator started serve thread")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	red_tasks_mu.Lock()
	defer red_tasks_mu.Unlock()
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	worker_id.Store(0)
	c := Coordinator{
		files:     files,
		map_tasks: make([]*Task, 0),
		remain:    len(files),
		nreduces:  nReduce,
	}
	map_tasks_mu.Lock()
	for _, file := range c.files {
		task_id++
		c.map_tasks = append(c.map_tasks, &Task{Name: MAP_TASK_TYPE, Arg_filename: file, Issued_at: nil, CurrentState: TASK_STATE_INIT, TaskId: task_id})
	}
	map_tasks_mu.Unlock()
	// fmt.Printf("Total map tasks %d\n", len(c.map_tasks))

	red_tasks_mu.Lock()
	for i := 0; i < nReduce; i++ {
		task_id++
		i_as_str := fmt.Sprintf("%d", i)
		c.red_tasks = append(c.red_tasks, &Task{Name: REDUCE_TASK_TYPE, Arg_filename: i_as_str, Issued_at: nil, CurrentState: TASK_STATE_INIT, TaskId: task_id})
	}
	red_tasks_mu.Unlock()
	// fmt.Println("Created coordinator")
	// fmt.Printf("Total red tasks %d\n", len(c.red_tasks))
	// Your code here.
	c.server()
	return &c
}
