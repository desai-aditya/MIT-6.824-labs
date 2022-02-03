package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	MAP = iota
	REDUCE 
	EXIT
)
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// RPC definitions for getting map file names from coordinator
type MapTaskReply struct {
	FilePath string
}

type MapTaskArgs struct {
	TaskNumber int
}

// RPC definitions for getting tasks from coordinator
type TaskTypeArgs struct {
}

type TaskTypeReply struct {
	TaskType int	
	TaskNumber int	
	NReduce int	
}




// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
