package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	NMap             int
	NReduce          int
	CurrentFileIndex int
	Files            []string
	Mapping          bool
	Reducing         bool
}

func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	if c.CurrentFileIndex == len(c.Files) {
		if c.Mapping {
			c.CurrentFileIndex = 0
			c.Mapping = false
			c.Reducing = true
		} else {
			reply.TaskType = "done"
			return nil
		}
	}
	if c.Mapping {
		reply.FileName = c.Files[c.CurrentFileIndex]
		reply.NReduce = c.NReduce
		reply.TaskType = "map"
		c.CurrentFileIndex += 1
	} else {
		reply.NMap = c.NMap
		reply.TaskType = "reduce"
		reply.CurrentFileIndex = c.CurrentFileIndex
		c.CurrentFileIndex += 1
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.Reducing && c.CurrentFileIndex == len(c.Files) {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:             len(files),
		NReduce:          nReduce,
		CurrentFileIndex: 0,
		Files:            files,
		Mapping:          true,
		Reducing:         false,
	}

	c.server(sockname)
	return &c
}
