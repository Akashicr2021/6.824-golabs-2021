package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.
type GetTaskArgs struct {
}

type Task struct {
	TaskInf
	Content []byte
}

type FinishTaskArgs struct {
	TaskID        int
	TaskType      int
	FileName      string //used for map TaskInf
	ReduceTaskNum int    //used for reduce TaskInf
}

type FinishTaskReply struct {
}

var MAP int = 0
var REDUCE int = 1
var NONE int = 2

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
