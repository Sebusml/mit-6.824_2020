package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	Unavailable TaskStatus = iota
	Ready
	InProgress
	Done
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Terminate
	Wait
)

type Task struct {
	id     int
	key    string
	worker int
	status TaskStatus
	// One solution is to have a go-routine cheking started at for all tasks and give up on them.
	startedAtEpoch int64
}

type Master struct {
	mu sync.Mutex
	// Your definitions here.
	mapTasks    []Task
	reduceTasks []Task
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) CompletedTask(request *TaskCompletedRequest, response *TaskCompletedResponse) error {
	fmt.Println("CompletedTask Handler called", request)
	m.mu.Lock()
	defer m.mu.Unlock()

	switch request.TaskType {
	case Map:
		for i, task := range m.mapTasks {
			if task.id == request.TaskId {
				m.mapTasks[i].status = Done
			}
		}
		response.TaskType = Wait
		return nil
	case Reduce:
		for i, task := range m.reduceTasks {
			if task.id == request.TaskId {
				m.reduceTasks[i].status = Done
			}
		}
		break
	default:
		return errors.New("CompletedTask called with invalid TaskType")
	}

	// Check if all reduce tasks are done
	allDone := true
	for _, task := range m.reduceTasks {
		if task.status != Done {
			allDone = false
		}
	}

	if allDone {
		response.TaskType = Terminate
	} else {
		response.TaskType = Wait
	}

	return nil
}

func (m *Master) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	fmt.Println("GetTask Called ")
	m.mu.Lock()
	defer m.mu.Unlock()

	// get next map task
	// TODO: use mutex
	for i, task := range m.mapTasks {
		if task.status == Ready {
			response.TaskType = Map
			response.TaskKey = task.key
			response.TaskId = task.id
			response.MrParamNorM = len(m.reduceTasks)

			m.mapTasks[i].status = InProgress
			m.mapTasks[i].worker = request.WorkerId
			m.mapTasks[i].startedAtEpoch = time.Now().Unix()

			return nil
		}
	}

	// Check all if all map tasks are done
	allDone := true
	for _, task := range m.mapTasks {
		if task.status != Done {
			allDone = false
		}
	}
	if !allDone {
		response.TaskType = Wait
		return nil
	}

	// ### All map operations are done!
	// TODO: use mutex
	for i, task := range m.reduceTasks {
		if task.status == Unavailable {
			response.TaskType = Reduce
			response.TaskId = task.id
			response.TaskKey = fmt.Sprintf("mr-*-%d", task.id)
			response.MrParamNorM = len(m.mapTasks)

			m.reduceTasks[i].status = InProgress
			m.reduceTasks[i].worker = request.WorkerId
			m.reduceTasks[i].startedAtEpoch = time.Now().Unix()

			return nil
		}
	}

	// Check all if all reduce tasks are done
	reduceDone := true
	for _, task := range m.mapTasks {
		if task.status != Done {
			reduceDone = false
		}
	}
	if !reduceDone {
		response.TaskType = Wait
		return nil
	}

	// All map and reduce tasks are completed
	response.TaskType = Terminate
	return nil
}

func (m *Master) assignMapTasks(files []string) {
	// Each file will be one Map task
	for i, filename := range files {
		m.mapTasks = append(m.mapTasks, Task{i, filename, -1, Ready, -1})
	}
}

func (m *Master) initializeReduceTasks(nReduce int) {
	for i := 0; i < nReduce; i += 1 {
		m.reduceTasks = append(m.reduceTasks, Task{i, "", -1, Unavailable, -1})
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	for _, task := range append(m.mapTasks, m.reduceTasks...) {
		if task.status != Done {
			return false
		}
	}

	return true
}

func (m *Master) initializeWatchdog() {
	go func() {
		for {
			m.mu.Lock()
			for i, task := range m.mapTasks {
				if task.startedAtEpoch != -1 && time.Since(time.Unix(task.startedAtEpoch, 0)) > time.Second*10 &&
					task.status == InProgress {
					m.mapTasks[i].startedAtEpoch = -1
					m.mapTasks[i].status = Ready
					m.mapTasks[i].worker = -1
				}
			}
			for i, task := range m.reduceTasks {
				if task.startedAtEpoch != -1 && time.Since(time.Unix(task.startedAtEpoch, 0)) > time.Second*10 &&
					task.status == InProgress {
					m.reduceTasks[i].startedAtEpoch = -1
					m.reduceTasks[i].status = Unavailable
					m.reduceTasks[i].worker = -1
				}
			}
			m.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.assignMapTasks(files)

	// number of reduce tasks are given
	m.initializeReduceTasks(nReduce)

	// Monitor stale tasks
	m.initializeWatchdog()

	m.server()
	return &m
}
