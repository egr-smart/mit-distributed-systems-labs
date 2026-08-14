package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	X int
}

type Reply struct {
	NReduce          int
	NMap             int
	TaskType         string
	FileName         string
	CurrentFileIndex int
}

// Add your RPC definitions here.
