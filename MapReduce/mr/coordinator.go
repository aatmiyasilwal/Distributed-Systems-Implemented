package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	nMap int // total number of map tasks
	cMap int // number of map tasks processed until now
	mapLog []int // 0: never touched, 1: in-progress, 2: completed

	nReduce int // number of reduce tasks
	cReduce int // number of reduce tasks processed until now
	reduceLog []int // 0: never touched, 1: in-progress, 2: completed
	
	files []string // input files
	mu sync.Mutex // mutex to protect shared state
}

func (c *Coordinator) RecieveMapComplete(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cMap++
	c.mapLog[args.TaskNumber] = 2 // mark the map task as completed
	return nil
}

func (c *Coordinator) RecieveReduceComplete(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cReduce++
	c.reduceLog[args.TaskNumber] = 2 // mark the reduce task as completed
	return nil
}

func (c *Coordinator) RequestTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	
	// Check if all maps are done (completed, not just assigned)
	if c.cMap < c.nMap {
		// assign a map task
		tba := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapLog[i] == 0 {
				tba = i
				break
			}
		}
		// if no map tasks are untouched, 
		if tba == -1 {
			reply.TaskStatus = 2 // waiting
			c.mu.Unlock()
		} else{
			// if a map task is yet to be touched
			c.mapLog[tba] = 1 // mark as in-progress
			reply.TaskStatus = 0 // this is a map task
			reply.Filename = c.files[tba]
			reply.CMap = tba
			reply.NReduce = c.nReduce
			c.mu.Unlock() // avoid deadlock with the go func below

			// if a map task is in-progress for 10 seconds, retry again
			go func(taskId int){	
				time.Sleep(time.Second * 10)
				c.mu.Lock()
				if c.mapLog[taskId] == 1 { // if the task is still in-progress
					c.mapLog[taskId] = 0 // mark it as untouched
				}
				c.mu.Unlock()
			}(tba)
		}
	} else if c.cMap == c.nMap && c.cReduce < c.nReduce {
		tba := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceLog[i] == 0 {
				tba = i
				break
			}
		}
		// if no reduce tasks are untouched
		if tba == -1 {
			reply.TaskStatus = 2
			c.mu.Unlock()
		} else {
			// if a reduce task is yet to be touched
			c.reduceLog[tba] = 1 // mark as in-progress
			reply.TaskStatus = 1 // this is a reduce task
			reply.CReduce = tba
			reply.NMap = c.nMap
			c.mu.Unlock() // avoid deadlock with the go func below

			go func(taskId int) {
				time.Sleep(time.Second * 10)
				c.mu.Lock()
				if c.reduceLog[taskId] == 1 { // if the task is still in-progress
					c.reduceLog[taskId] = 0 // mark it as untouched
				}
				c.mu.Unlock()
			}(tba)
		}	
	} else {
		// all tasks are completed
		reply.TaskStatus = 3
		c.mu.Unlock()
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nReduce == c.cReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nMap = len(files)
	c.mapLog = make([]int, c.nMap)
	c.nReduce = nReduce
	c.reduceLog = make([]int, c.nReduce)
	c.files = files

	c.server()
	return &c
}
