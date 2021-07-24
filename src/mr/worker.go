package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// TaskInf number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for true {
		args := GetTaskArgs{}
		reply := Task{}
		call("Coordinator.GetTask", &args, &reply)

		if reply.TaskType == MAP {
			content := mapTaskContent{}
			if err := json.Unmarshal(reply.Content, &content); err != nil {
				fmt.Errorf("unmarshal fail")
			}

			onameBase := "mr-" + strconv.Itoa(content.FileIndex) + "-"
			encoders := make([]*json.Encoder, content.NReduce)
			for i := 0; i < content.NReduce; i++ {
				oname := onameBase + strconv.Itoa(i)
				ofile, _ := os.Create(oname)
				encoders[i] = json.NewEncoder(ofile)
			}

			kva := mapf(content.FileName, content.Input)

			for _, kv := range kva {
				encoder := encoders[ihash(kv.Key)%content.NReduce]
				encoder.Encode(&kv)
			}

			finishTaskArgs := FinishTaskArgs{
				TaskType: MAP,
				TaskID:   reply.TaskID,
			}
			finishTaskReply := FinishTaskReply{}

			call("Coordinator.FinishTask", &finishTaskArgs, &finishTaskReply)

		} else if reply.TaskType == REDUCE {
			content := reduceTaskContent{}
			if err := json.Unmarshal(reply.Content, &content); err != nil {
				fmt.Errorf("unmarshal fail")
			}

			intermediate := []KeyValue{}
			for i:=0;i<content.FileNum;i++ {
				filename:="mr-" + strconv.Itoa(i) + "-"+strconv.Itoa(content.ReduceTaskNum)
				file,_:=os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			ofileName:="mr-out-"+strconv.Itoa(content.ReduceTaskNum)
			ofile,_:=os.Create(ofileName)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}


			finishTaskArgs := FinishTaskArgs{
				TaskType: REDUCE,
				TaskID:   reply.TaskID,
			}
			finishTaskReply := FinishTaskReply{}
			call("Coordinator.FinishTask", &finishTaskArgs, &finishTaskReply)
		}

		time.Sleep(1 * time.Second)

	}
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
