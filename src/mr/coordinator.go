package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	IDLE = iota
	INPROGRESS
	DONE
)

type Coordinator struct {
	// Your definitions here.
	nMap,nReduce int
	files []string
    nextFileIndex int
	mu sync.Mutex
	mapTaskList []int
	reduceTaskList []int
	numWorkers int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 
// Function to check if any map tasks are idle
//
func (c *Coordinator) anyMapIdle() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskList[i] == IDLE{
				return true 
			}
		}
		return false 
}

// 
// Function to check if any reduce tasks are idle
//
func (c *Coordinator) anyReduceIdle() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskList[i] == IDLE {
				return true
			}
		}
		return false
}

// 
// Function to check if all map tasks are done
//
func (c *Coordinator) allMapDone() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskList[i] != DONE {
				return false
			}
		}
		return true 
}


// 
// Function to check if all reduce tasks are done
//
func (c *Coordinator) allReduceDone() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskList[i] != DONE{
				return false
			}
		}
		return true 
}

//
// RPC handler that gives out task types to workers
// Remember handling the timeout when worker crashes
func (c *Coordinator) GiveTaskType(args *TaskTypeArgs, reply *TaskTypeReply) error {
	
	if c.anyMapIdle() {
		c.mu.Lock()
		defer c.mu.Unlock()
		reply.TaskType = MAP 
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskList[i] == IDLE {
				c.mapTaskList[i] = INPROGRESS
				reply.TaskNumber = i
				reply.NReduce = c.nReduce
				return nil
			}
		}
	}

	if c.anyReduceIdle(){
		c.mu.Lock()
		defer c.mu.Unlock()
		reply.TaskType = REDUCE
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskList[i] == IDLE {
				c.reduceTaskList[i] = INPROGRESS
				reply.TaskNumber = i
				return nil
			}
		}

	}

	reply.TaskType = EXIT
	return nil
}


//
// RPC handler that gives out files to Map tasks.
//
func (c *Coordinator) GiveMapFiles(args *MapTaskArgs, reply *MapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskNumber >= len(c.files) { 
		return errors.New("Index out of bound")
	}
	reply.FilePath = c.files[args.TaskNumber]
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.allMapDone() && c.allReduceDone()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce:nReduce, files:files, nMap:len(files), nextFileIndex: 0, mapTaskList: make([]int,len(files)),reduceTaskList: make([]int,nReduce),numWorkers: 0  }

	// Your code here.
	for i := 0; i < len(files); i++ {
		c.mapTaskList[i] = IDLE
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskList[i] = IDLE
	}

	c.server()
	return &c
}
