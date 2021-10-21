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

type GetTaskRequest struct {
	WorkerId int
}

type GetTaskResponse struct {
	TaskType    TaskType
	TaskKey     string
	TaskId      int
	MrParamNorM int
}

type TaskCompletedRequest struct {
	TaskType TaskType
	TaskId   int
	WorkerId int
}

type TaskCompletedResponse struct {
	TaskType TaskType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
