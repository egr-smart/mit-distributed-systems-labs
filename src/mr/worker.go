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

		for i := 0; i < reply.NReduce; i++ {
			files[i].Close()
			os.Rename(files[i].Name(), fmt.Sprintf("mr-%x-%x", reply.CurrentFileIndex, i))
		}

		ReportComplete(reply.CurrentFileIndex, reply.TaskType)
	case "reduce":
		oname := "mr-out-0"
		ofile, _ := os.Create(oname)

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

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
	case "done":
	}
}

func ReportComplete(taskNo int, taskType string) {
	msg := UpdateMessage{}
	msg.TaskNo = taskNo
	msg.TaskType = taskType

	reply := UpdateRecieved{}

	ok := call("Coordinator.ReportComplete", &msg, &reply)
	if ok {
		if !reply.Acknowledge {
			fmt.Printf("report complete not acknowledged\n")
		}
		return
	} else {
		fmt.Printf("call to report complete failed!\n")
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
		fmt.Printf("call to get task failed!\n")
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
