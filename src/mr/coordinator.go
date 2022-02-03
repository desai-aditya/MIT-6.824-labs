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


type Coordinator struct {
	// Your definitions here.
	nMap,nReduce int
	files []string
    nextFileIndex int
	mu sync.Mutex
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
// RPC handler that gives out files to Map tasks.
//
func (c *Coordinator) GiveMapFiles(args *MapTaskArgs, reply *MapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nextFileIndex >= len(c.files) { 
		return errors.New("Index out of bound")
	}
	reply.FilePath = c.files[c.nextFileIndex]
	c.nextFileIndex++
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
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce:nReduce, files:files, nMap:len(files), nextFileIndex: 0}

	// Your code here.


	c.server()
	return &c
}
