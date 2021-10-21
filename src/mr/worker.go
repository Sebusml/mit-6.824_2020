package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rand.Seed(0)
	workerId := rand.Int()
	// Loop asking master for tasks
	for CallMasterAndExecute(mapf, reducef, workerId) {
		time.Sleep(time.Second)
	}
	// Reducers should create output with following format mr-out-X
	fmt.Println("Closing worker")
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMasterAndExecute(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, workerId int) bool {

	getTaskRequest := GetTaskRequest{workerId}
	getTaskResponse := GetTaskResponse{}

	// send the RPC request, wait for the reply.
	call("Master.GetTask", &getTaskRequest, &getTaskResponse)

	switch getTaskResponse.TaskType {
	case Wait:
		return true
	case Terminate:
		return false
	case Map:
		taskId := getTaskResponse.TaskId
		filename := getTaskResponse.TaskKey
		NReduce := getTaskResponse.MrParamNorM

		// read file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf(err.Error())
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))
		//sort.Sort(ByKey(kva))

		// Dump kvs in NxM temp files using json format
		tmpFiles := map[string]*os.File{}
		jsonEncoders := map[string]*json.Encoder{}
		for i := 0; i < NReduce; i += 1 {
			filename := fmt.Sprintf("mr-%d-%d", taskId, i)
			tempFile, err := ioutil.TempFile("", filename)
			if err != nil {
				log.Fatalf(err.Error())
			}
			tmpFiles[filename] = tempFile
			jsonEncoders[filename] = json.NewEncoder(tempFile)
		}

		for _, kv := range kva {
			reduceIndex := ihash(kv.Key) % NReduce
			filename := fmt.Sprintf("mr-%d-%d", taskId, reduceIndex)

			jsonError := jsonEncoders[filename].Encode(&kv)
			if jsonError != nil {
				log.Fatalf(jsonError.Error())
			}
		}

		// Close files and copy to local dir
		for filename, file := range tmpFiles {
			file.Close()
			// Atomically copy file to current folder
			os.Rename(file.Name(), filename)
		}

		// Tell master that task is completed
		completedRequest := TaskCompletedRequest{Map, taskId, workerId}
		completedResponse := TaskCompletedResponse{}
		call("Master.CompletedTask", &completedRequest, &completedResponse)

		if completedResponse.TaskType != Wait {
			log.Fatalf("Completed response is not OK: value= %v", completedResponse.TaskType)
		}
		return true
	case Reduce:
		// Read all intermediate files
		taskId := getTaskResponse.TaskId
		fileRegex := getTaskResponse.TaskKey
		MMapTasks := getTaskResponse.MrParamNorM

		// Parse each MapFile
		allKv := []KeyValue{}
		for i := 0; i < MMapTasks; i += 1 {
			filename := strings.Replace(fileRegex, "*", strconv.Itoa(i), -1)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf(err.Error())
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				allKv = append(allKv, kv)
			}
		}

		// Run Reduce Operation and write to file
		sort.Sort(ByKey(allKv))

		oname := "mr-out-" + strconv.Itoa(taskId)
		tmpFile, _ := ioutil.TempFile("", oname)

		i := 0
		for i < len(allKv) {
			j := i + 1
			for j < len(allKv) && allKv[j].Key == allKv[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, allKv[k].Value)
			}
			output := reducef(allKv[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpFile, "%v %v\n", allKv[i].Key, output)

			i = j
		}

		tmpFile.Close()
		os.Rename(tmpFile.Name(), oname)

		// Call Master with reduce task finished

		completedRequest := TaskCompletedRequest{Reduce, taskId, workerId}
		completedResponse := TaskCompletedResponse{}
		call("Master.CompletedTask", &completedRequest, &completedResponse)

		switch completedResponse.TaskType {
		case Wait:
			return true
		case Terminate:
			return false
		default:
			log.Fatalf("Invalid response from CompletedTask Reduce")
		}
	default:
		log.Fatalf("Worker got invalid Task From Master")
	}
	return false
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
