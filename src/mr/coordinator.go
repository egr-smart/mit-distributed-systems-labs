package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	NMap               int
	NReduce            int
	CurrentFileIndex   int
	CurrentBucketIndex int
	Files              []string
	Mapping            bool
	Reducing           bool
	MapTaskList        []Task
	ReduceTaskList     []Task
}

type Task struct {
	TaskNo     int
	Status     string
	AssignedAt time.Time
}

func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	for i, task := range c.MapTaskList {
		switch task.Status {
		case "idle":
			reply.CurrentFileIndex = i
			reply.NReduce = c.NReduce
			reply.TaskType = "map"
			return nil
		case "in-progress":
			if time.Since(task.AssignedAt) > 10*time.Second {
				task.Status = "idle"
			}
			reply.TaskType = "wait"
			return nil
		}
	}

	if c.Mapping {
		if c.CurrentFileIndex == len(c.Files) {
			c.Mapping = false
			c.Reducing = true
		}
		reply.FileName = c.Files[c.CurrentFileIndex]
		reply.NReduce = c.NReduce
		reply.TaskType = "map"
		c.CurrentFileIndex += 1
	}
	if c.Reducing {
		if c.CurrentBucketIndex == c.NReduce {
			reply.TaskType = "done"
			return nil
		}
		reply.NMap = c.NMap
		reply.TaskType = "reduce"
		reply.CurrentBucketIndex = c.CurrentBucketIndex
		c.CurrentBucketIndex += 1
	}
	return nil
}

func (c *Coordinator) ReportComplete(args *UpdateMessage, reply UpdateRecieved) error {
	if args.TaskType == "map" {
		c.MapTaskList[args.TaskNo].Status = "complete"
	} else {
		c.ReduceTaskList[args.TaskNo].Status = "complete"
	}
	reply.Acknowledge = true
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
	if c.Reducing && c.CurrentBucketIndex == len(c.Files) {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:               len(files),
		NReduce:            nReduce,
		CurrentFileIndex:   0,
		CurrentBucketIndex: 0,
		Files:              files,
		Mapping:            true,
		Reducing:           false,
	}

	c.server(sockname)
	return &c
}
