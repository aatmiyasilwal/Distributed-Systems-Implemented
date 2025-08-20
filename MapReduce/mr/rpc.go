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

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type WorkerArgs struct {
	TaskNumber int // task number
}

type WorkerReply struct {
	TaskStatus int // 0: map, 1: reduce, 2: waiting, 3: completed

	NMap int // total number of map tasks
	CMap int // current map task number

	NReduce int // total number of reduce tasks
	CReduce int // current reduce task number
	
	Filename string // name of the file to process

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
