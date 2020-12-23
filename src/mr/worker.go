package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
// perform a Map task. Reads from mFile, Generates nReduce files with
// names mr-X-Y, where X is the Map task number, and Y is the reduce task number.
//
func doMap(mid int, nReduce int, mFile string, mapf func(string, string) []KeyValue) bool {
	file, err := os.Open(mFile)
	if err != nil {
		log.Fatalf("cannot open %v", mFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mFile)
	}
	file.Close()
	kva := mapf(mFile, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		ind := ihash(kv.Key) % nReduce
		buckets[ind] = append(buckets[ind], kv)
	}
	for rid, kva := range buckets {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", mid, rid)
		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot create %v", intermediateFileName)
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write kv pair %v", kv)
			}
		}
		intermediateFile.Close()
	}

	return true
}

//
// perform a Reduce task. Reads nMap files with names mr-X-Y, and
// Generates one file named mr-out-Y, where X is the Map task number,
// and Y is the reduce task number.
//
func doReduce(rid int, nMap int, reducef func(string, []string) string) bool {
	var intermediate []KeyValue
	for mid:=0; mid<nMap; mid++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", mid, rid)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		intermediateFile.Close()
	}
	sort.Sort(ByKey(intermediate))

	outName := fmt.Sprintf("mr-out-%d", rid)
	outFile, _ := os.Create(outName)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write output %v", output)
		}
		i = j
	}

	outFile.Close()
	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	RpcRetryLimit := 3
	var rpcRetries int
	for {
		assignment, ok := CallAssignment()
		// If everything is complete, exit the operation
		// RPC call failures have are allowed multiple retries
		if assignment.TaskType == Exit || (!ok && rpcRetries == RpcRetryLimit) {
			return
		} else if !ok {
			rpcRetries++
			time.Sleep(time.Second)
			continue
		}
		rpcRetries = 0
		// Currently, if a task fails, we don't report the failure back to master.
		// master controller will find the task timed out and reset it back to Idle.
		// In a real system, it would be helpful to report the error back to master
		// as well.
		if assignment.TaskType == Map {
			if doMap(assignment.MapId, assignment.ReduceCount, assignment.MapFile, mapf) {
				for !CallReportComplete(Map, assignment.MapId) {
					if rpcRetries == RpcRetryLimit {
						break
					}
					rpcRetries ++
					time.Sleep(time.Second)
				}
				rpcRetries = 0
			}
		} else if assignment.TaskType == Reduce {
			if doReduce(assignment.ReduceId, assignment.MapCount, reducef) {
				for !CallReportComplete(Reduce, assignment.ReduceId) {
					if rpcRetries == RpcRetryLimit {
						break
					}
					rpcRetries ++
					time.Sleep(time.Second)
				}
				rpcRetries = 0
			}
		} else {
			log.Fatalf("Invalid TaskType assigned: %s", assignment.TaskType)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallAssignment() (*AssignmentReply, bool) {
	args := AssignmentArgs{}
	reply := AssignmentReply{}
	if call("Master.Assignment", &args, &reply) {
		return &reply, true
	}
	return nil, false
}

func CallReportComplete(taskType TaskType, taskId int) bool {
	args := ReportCompleteArgs{
		TaskType: taskType,
		TaskId: taskId,
	}
	reply := ReportCompleteReply{}
	return call("Master.ReportComplete", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
