package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	reply := GetTask()

	switch reply.TaskType {
	case "map":
		content := readFile(reply.FileName)
		kva := mapf(reply.FileName, content)
		files := make([]*os.File, reply.NReduce)
		encoders := make([]*json.Encoder, reply.NReduce)
		// create files and encoders
		for i := 0; i < reply.NReduce; i++ {
			file, err := os.CreateTemp(".", "mr-tmp-*")
			if err != nil {
				log.Fatalf("cannot write %v", file.Name())
			}
			files[i] = file
			encoders[i] = json.NewEncoder(file)
		}

		for _, kv := range kva {
			taskno := ihash(kv.Key) % reply.NReduce
			err := encoders[taskno].Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to %v", files[taskno].Name())
			}
		}
	case "reduce":
	case "done":
	}
}

func GetTask() Reply {
	args := Args{}

	args.X = 0

	reply := Reply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
