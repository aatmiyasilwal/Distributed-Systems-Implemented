package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	for {
		args := WorkerArgs{}
		reply := WorkerReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok || reply.TaskStatus == 3 {
			break // all tasks completed
		}
		switch reply.TaskStatus {
			case 0:
				// map task
				intermediate := []KeyValue{}
				file, err := os.Open(reply.Filename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Filename)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filename)
				}
				file.Close()

				// call the map function
				kva := mapf(reply.Filename, string(content))
				intermediate = append(intermediate, kva...)

				// partition the intermediate key/value pairs into NReduce buckets,
				// and send each bucket to the coordinator.
				buckets := make([][]KeyValue, reply.NReduce)
				for _, kv := range intermediate {
					r := ihash(kv.Key) % reply.NReduce
					buckets[r] = append(buckets[r], kv)
				}

				// write to intermediate files
				for i, bucket := range buckets {
					oname := fmt.Sprintf("mr-%d-%d", reply.CMap, i)
					ofile, _ := os.CreateTemp("", oname+"*")
					enc := json.NewEncoder(ofile)
					for _, kv := range bucket {
						if err := enc.Encode(&kv); err != nil {
							log.Fatalf("cannot write into %v", oname)
						}
					}
					ofile.Close()
					os.Rename(ofile.Name(), oname) // atomic rename to final name
				}
				finishedArgs := WorkerArgs{
					TaskNumber: reply.CMap,
				}
				finishedReply := WorkerReply{}
				call("Coordinator.RecieveMapComplete", &finishedArgs, &finishedReply)
				
			case 1:
				// reduce task
				intermediate := []KeyValue{}
				for i := 0; i < reply.NMap; i++ {
					iname := fmt.Sprintf("mr-%d-%d", i, reply.CReduce)
					ifile, err := os.Open(iname)
					// if the file does not exist, skip it
					if err != nil {
						continue
					}
					// read the intermediate key/value pairs from the file
					dec := json.NewDecoder(ifile)
					for {
						var kv KeyValue
						// decode each key/value pair from the file
						// if EOF is reached, break the loop
						if err := dec.Decode(&kv); err != nil {
							break
						}
						
						intermediate = append(intermediate, kv)
					}
					ifile.Close()
				}
				// sort the intermediate key/value pairs by key
				sort.Sort(ByKey(intermediate))

				oname := fmt.Sprintf("mr-out-%d", reply.CReduce)
				ofile, _ := os.CreateTemp("", oname+"*")
				// call the reduce function on each distinct key in intermediate[]
				i := 0
				for i < len(intermediate) {
					j := i + 1
					// find the end of the current key
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					// collect all values for the current key
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					// call the reduce function
					output := reducef(intermediate[i].Key, values)
					// write the output to the file
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j // move to the next key
				}
				ofile.Close()
				os.Rename(ofile.Name(), oname) // atomic rename to final name

				// Clean up intermediate files (ignore errors for missing files)
				for i := 0; i < reply.NMap; i++ {
					iname := fmt.Sprintf("mr-%d-%d", i, reply.CReduce)
					os.Remove(iname) // remove the intermediate files, ignore errors
				}

				// notify the coordinator that the reduce task is complete
				finishedArgs := WorkerArgs{
					TaskNumber: reply.CReduce,
				}
				finishedReply := WorkerReply{}
				call("Coordinator.RecieveReduceComplete", &finishedArgs, &finishedReply)
		}
	}
	
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

	fmt.Println(err)
	return false
}
