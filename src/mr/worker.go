package mr

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

	for {
		switch nReduce,taskType, taskNumber := CallGetTask(); taskType{
		case MAP:
			PerformMap(nReduce,taskNumber, mapf)
		case REDUCE:

		case EXIT:
			return
		default:
			log.Fatalf("No such task exists: %v\n",taskType)
		}
		time.Sleep(time.Second)
	}

}

//
// This function performs the Map operation as specified in the Mapf pointer
//
// read each input file,
// pass it to Map,
// accumulate the intermediate Map output.
//
func PerformMap(nReduce int, taskNumber int, mapf func(string,string) []KeyValue) []KeyValue{
    //
	filename := CallGetFileName(taskNumber)
    intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	var intermediates_array = make([][]KeyValue,nReduce)
	for i := 0; i < len(intermediate); i++ {
		key := intermediate[i].Key
		intermediates_array[ ihash(key)%nReduce] = append(intermediates_array[ihash(key)%nReduce],intermediate[i])
	}

	for i:=0; i < len(intermediates_array); i++ {

		storefile , err := os.OpenFile("mr-"+ strconv.Itoa(taskNumber) + "-"+strconv.Itoa(i),os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
		if err != nil {
			log.Printf("error %v", err)
			log.Fatalf("cannot open %v", storefile)
		}
		enc := json.NewEncoder(storefile)
		for _, kv := range intermediates_array[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("error %v", err)
				log.Fatalf("cannot encode key value\n")
			}
		}
	}

	return intermediate
}

//
// function to get a either a map or a reduce task from the coordinator.
//
func CallGetTask() (int,int,int) {

	// declare an argument structure.
	args := TaskTypeArgs{}

	// fill in the argument(s). But there are no args so empty.

	// declare a reply structure.
	reply := TaskTypeReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GiveTaskType", &args, &reply)

	log.Printf("reply.TaskType %v\n",reply.TaskType)
	log.Printf("reply.TaskNumber %v\n", reply.TaskNumber)
	log.Printf("reply.nReduce %v\n", reply.NReduce)

	return reply.NReduce, reply.TaskType,reply.TaskNumber
}

//
// function to get a file split from the coordinator.
//
func CallGetFileName(taskNumber int) string {

	// declare an argument structure.
	args := MapTaskArgs{}

	// fill in the argument(s).
	args.TaskNumber = taskNumber

	// declare a reply structure.
	reply := MapTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GiveMapFiles", &args, &reply)

	// Print reply
	log.Printf("reply.FilePath %v\n", reply.FilePath)

	return reply.FilePath
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	log.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
