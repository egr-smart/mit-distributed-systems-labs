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
	NMap           int
	NReduce        int
	Files          []string
	Mapping        bool
	Complete       bool
	MapTaskList    []Task
	ReduceTaskList []Task
}

type Task struct {
	Status     string
	AssignedAt time.Time
}

func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	if c.Mapping {
		notComplete := false
		for i, task := range c.MapTaskList {
			if task.Status == "in-progress" {
				if time.Since(task.AssignedAt) > 10*time.Second {
					c.MapTaskList[i].Status = "idle"
				} else {
					notComplete = true
				}
			}
		}

		for i, task := range c.MapTaskList {
			if task.Status == "idle" {
				reply.TaskNo = i
				reply.NReduce = c.NReduce
				reply.FileName = c.Files[i]
				reply.TaskType = "map"
				c.MapTaskList[i].AssignedAt = time.Now()
				c.MapTaskList[i].Status = "in-progress"
				return nil
			}
		}

		if notComplete {
			reply.TaskType = "wait"
			return nil
		} else {
			c.Mapping = false
		}
	}

	if !c.Complete {
		notComplete := false
		for i, task := range c.ReduceTaskList {
			if task.Status == "in-progress" {
				if time.Since(task.AssignedAt) > 10*time.Second {
					c.ReduceTaskList[i].Status = "idle"
				} else {
					notComplete = true
				}
			}
		}

		for i, task := range c.ReduceTaskList {
			if task.Status == "idle" {
				reply.NMap = c.NMap
				reply.TaskType = "reduce"
				reply.TaskNo = i
				c.ReduceTaskList[i].AssignedAt = time.Now()
				c.ReduceTaskList[i].Status = "in-progress"
				return nil
			}
		}

		if notComplete {
			reply.TaskType = "wait"
			return nil
		} else {
			c.Complete = true
		}
	}
	reply.TaskType = "done"
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
	if c.Complete {
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
