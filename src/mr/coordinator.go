package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	Mu             sync.Mutex
	NMap           int
	NReduce        int
	Files          []string
	Mapping        bool
	Complete       bool
	MapTaskList    []Task
	ReduceTaskList []Task
}
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Complete
)

type Task struct {
	Status     TaskStatus
	AssignedAt time.Time
}

func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if c.Mapping {
		notComplete := false
		for i, task := range c.MapTaskList {
			if task.Status == InProgress {
				if time.Since(task.AssignedAt) > 10*time.Second {
					c.MapTaskList[i].Status = Idle
				} else {
					notComplete = true
				}
			}
		}

		for i, task := range c.MapTaskList {
			if task.Status == Idle {
				reply.TaskNo = i
				reply.NReduce = c.NReduce
				reply.FileName = c.Files[i]
				reply.TaskType = "map"
				c.MapTaskList[i].AssignedAt = time.Now()
				c.MapTaskList[i].Status = InProgress
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
			if task.Status == InProgress {
				if time.Since(task.AssignedAt) > 10*time.Second {
					c.ReduceTaskList[i].Status = Idle
				} else {
					notComplete = true
				}
			}
		}

		for i, task := range c.ReduceTaskList {
			if task.Status == Idle {
				reply.NMap = c.NMap
				reply.TaskType = "reduce"
				reply.TaskNo = i
				c.ReduceTaskList[i].AssignedAt = time.Now()
				c.ReduceTaskList[i].Status = InProgress
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

func (c *Coordinator) ReportComplete(args *UpdateMessage, reply *UpdateRecieved) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if args.TaskType == "map" {
		c.MapTaskList[args.TaskNo].Status = Complete
	} else {
		c.ReduceTaskList[args.TaskNo].Status = Complete
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
	c.Mu.Lock()
	defer c.Mu.Unlock()
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
	mapTaskList := make([]Task, len(files))
	reduceTaskList := make([]Task, nReduce)
	c := Coordinator{
		NMap:           len(files),
		NReduce:        nReduce,
		Files:          files,
		Mapping:        true,
		Complete:       false,
		MapTaskList:    mapTaskList,
		ReduceTaskList: reduceTaskList,
	}

	c.server(sockname)
	return &c
}
