package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int

	files []string

	taskChan chan *Task

	tasksMap map[int]*Task

	finishedTaskCounter int

	allMapTasksDoneNotifier chan int

	allTasksDoneNotifier chan int

	taskMapMu sync.Mutex
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
	<-c.allTasksDoneNotifier
	ret=true

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.init(files, nReduce)

	go c.pushTaskToChan()

	c.server()
	return &c
}

//my definitions
var WORKING int = 0
var FINISHED int = 1

type TaskInf struct {
	TaskID   int
	TaskType int
	Status   int
	mu       sync.Mutex
}

type mapTaskContent struct {
	FileName  string
	NReduce   int
	Input     string
	FileIndex int
}

type reduceTaskContent struct {
	ReduceTaskNum int
	FileNum       int
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.files = files
	c.nReduce = nReduce
	c.taskChan = make(chan *Task, 1)
	c.allMapTasksDoneNotifier = make(chan int, 1)
	c.allTasksDoneNotifier = make(chan int, 1)
	c.tasksMap = make(map[int]*Task)
}

func (c *Coordinator) createMapTask(taskID int, fileName string, mapTaskNum int) (
	*Task, error,
) {
	content := mapTaskContent{
		FileName:  fileName,
		NReduce:   c.nReduce,
		FileIndex: mapTaskNum,
	}
	task := Task{
		TaskInf: TaskInf{
			TaskType: MAP,
			TaskID:   taskID,
			Status:   WORKING,
		},
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	fileRawBytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	content.Input = string(fileRawBytes)
	task.Content, err = json.Marshal(content)

	return &task, err
}

func (c *Coordinator) createReduceTask(taskID int, reduceTaskNum int) (*Task, error) {
	content := reduceTaskContent{
		ReduceTaskNum: reduceTaskNum,
		FileNum:       len(c.files),
	}
	task := Task{
		TaskInf: TaskInf{
			TaskType: REDUCE,
			TaskID:   taskID,
			Status:   WORKING,
		},
	}

	var err error
	task.Content, err = json.Marshal(content)

	return &task, err
}

func (c *Coordinator) pushTaskToChan() {
	id := 0
	for _, file := range c.files {
		task, err := c.createMapTask(id, file, id)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}

		c.taskMapMu.Lock()
		c.tasksMap[id] = task
		c.taskMapMu.Unlock()

		c.taskChan <- task

		id++
	}
	//TODO
	//wait for worker finishing map and then push reduce TaskInf
	<-c.allMapTasksDoneNotifier

	for i := 0; i < c.nReduce; i++ {
		task, err := c.createReduceTask(id, i)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}

		c.taskMapMu.Lock()
		c.tasksMap[id] = task
		c.taskMapMu.Unlock()

		c.taskChan <- task

		id++
	}

}

func (c *Coordinator) superviseTask(t *Task) {
	timer := time.NewTimer(time.Second * 10)
	<-timer.C

	t.mu.Lock()
	if t.Status == WORKING {
		c.taskChan <- t
	}
	t.mu.Unlock()

}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *Task) error {
	select {
	case t := <-c.taskChan:
		t.Status = WORKING
		reply.TaskID=t.TaskID
		reply.TaskType=t.TaskType
		reply.Status=t.Status
		reply.Content=t.Content

		go c.superviseTask(t)

	default:
		reply.TaskType = NONE
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.taskMapMu.Lock()
	t, ok := c.tasksMap[args.TaskID]
	c.finishedTaskCounter++
	if !ok {
		fmt.Println("TaskInf id does not exist")
		os.Exit(-1)
	}
	if c.finishedTaskCounter == len(c.files) {
		c.allMapTasksDoneNotifier <- 1
	}
	if c.finishedTaskCounter == len(c.files)+c.nReduce {
		c.allTasksDoneNotifier <- 1
	}
	c.taskMapMu.Unlock()

	t.mu.Lock()
	t.Status = FINISHED
	t.mu.Unlock()

	return nil
}

