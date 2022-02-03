package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE = iota
	INPROGRESS
	DONE
)

const TIMEOUT = 10

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
// RPC handler for the case when workers report task is done
//
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MAP:
		c.mapTaskList[args.TaskNumber] = DONE	

	case REDUCE:
		c.reduceTaskList[args.TaskNumber] = DONE	

	default:
		log.Fatalf("No such task %v\n",args.TaskType)
	}
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
// Function to trigger a timeout after waiting for sometime 
//
// resets its status to IDLE
//
func (c *Coordinator) TriggerTimeout(taskNumber int, taskType int) {

	time.Sleep(TIMEOUT * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	switch taskType{
	case MAP:
		if c.mapTaskList[taskNumber] != DONE {
			c.mapTaskList[taskNumber] = IDLE
		}
		
	case REDUCE:
		if c.reduceTaskList[taskNumber] != DONE {
			c.reduceTaskList[taskNumber] = IDLE
		}

	default:
		log.Fatalf("no such task type %v\n",taskType)

	}

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
				reply.NMap = c.nMap
				go c.TriggerTimeout(reply.TaskNumber, reply.TaskType)
				return nil
			}
		}
	}

	if !c.allMapDone() {
		reply.TaskType = WAIT
		return nil
	}

	if c.anyReduceIdle(){
		c.mu.Lock()
		defer c.mu.Unlock()
		reply.TaskType = REDUCE
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskList[i] == IDLE {
				c.reduceTaskList[i] = INPROGRESS
				reply.TaskNumber = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				go c.TriggerTimeout(reply.TaskNumber, reply.TaskType)
				return nil
			}
		}

	}

	if !c.allReduceDone() {
		reply.TaskType = WAIT
		return nil
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
// function for debugging to print internal state of the coordinator
//
func (c *Coordinator) PrintState() {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("Map State\n")
	for i :=0 ; i < c.nMap; i++ {
		str := "none"
		switch c.mapTaskList[i] {
		case IDLE:
			str = "idle"
		case INPROGRESS:
			str = "inprogress"
		case DONE:
			str = "done"
		}
		fmt.Printf("i = %v, val = %v\n",i,str)
	}
	fmt.Printf("Reduce State\n")
	for i :=0 ; i < c.nReduce; i++ {
		str := "none"
		switch c.reduceTaskList[i] {
		case IDLE:
			str = "idle"
		case INPROGRESS:
			str = "inprogress"
		case DONE:
			str = "done"
		}
		fmt.Printf("i = %v, val = %v\n",i,str)
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.

	//c.PrintState()
	//time.Sleep(10*time.Second)


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
