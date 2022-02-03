package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		switch nMap,nReduce,taskType, taskNumber := CallGetTask(); taskType{
		case MAP:
			PerformMap(nReduce,taskNumber, mapf)
		case REDUCE:
			PerformReduce(nMap,taskNumber,reducef)
		case EXIT:
			return
		default:
			log.Fatalf("No such task exists: %v\n",taskType)
		}
		time.Sleep(time.Second)
	}

}

//
// This function performs the Map operation as specified in the mapf pointer
//
// read each input file,
// pass it to Map,
// accumulate the intermediate Map output in separate files
//
func PerformMap(nReduce int, taskNumber int, mapf func(string,string) []KeyValue) {
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
		storefile.Close()
	}

	CallTaskDone(MAP, taskNumber)

}

//
// This function performs the Reduce operation as specified in the reducef pointer
//
// read intermediate output from map
// sort intermediate keys
// pass it to reduce
// accumulate the output
//
func PerformReduce(nMap int, taskNumber int, reducef func(string, []string) string) {

	var intermediate []KeyValue
	// Read intermediate file output from map
	for i:=0 ; i < nMap; i++ {

		ifilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNumber)
		ifile, err := os.Open(ifilename)
		if err != nil {
			log.Printf("error %v", err)
			log.Fatalf("cannot open %v", ifile)
		}

		dec := json.NewDecoder(ifile)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate,kv)
		}

	}


	// sort intermediate output
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(taskNumber)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-R
	//
	i := 0
	for i < len(intermediate) {
		//log.Printf("%v %v\n",intermediate[i].Key, intermediate[i].Value)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	CallTaskDone(REDUCE, taskNumber)

}

//
// function to acknowledge that task is done
//
func CallTaskDone(taskType int, taskNumber int) {

	// declare an argument structure.
	args := TaskDoneArgs{}

	// fill in the argument(s).
	args.TaskType = taskType
	args.TaskNumber = taskNumber

	// declare a reply structure.
	reply := TaskTypeReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.TaskDone", &args, &reply)
}

//
// function to get a either a map or a reduce task from the coordinator.
//
func CallGetTask() (int,int,int,int) {

	// declare an argument structure.
	args := TaskTypeArgs{}

	// fill in the argument(s). But there are no args so empty.

	// declare a reply structure.
	reply := TaskTypeReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GiveTaskType", &args, &reply)

	log.Printf("reply.TaskType %v\n",reply.TaskType)
	log.Printf("reply.TaskNumber %v\n", reply.TaskNumber)
	log.Printf("reply.nMap %v\n", reply.NMap)
	log.Printf("reply.nReduce %v\n", reply.NReduce)

	return reply.NMap, reply.NReduce, reply.TaskType,reply.TaskNumber
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
