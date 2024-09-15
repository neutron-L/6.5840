package mr

import (
	"log"
	"net"
	"os"
	"sync"
	"net/rpc"
	"net/http"
	"time"
)


type taskStatus int

type Coordinator struct {
	// Your definitions here.
	phase   	jobPhase
	hasDoneNum	int
	hasDone		[]taskStatus      // 任务完成情况
	files 		[]string	      // input files
	task_num	int				  // nMap or nReduce
	nMap		int
	nReduce		int

	mtx			sync.Mutex
}

const (
	UNDO = 1
	DOING = 2
	DONE = 3
)


const (
	MAP = "Map"
	REDUCE = "Reduce"
	EXIT = "Exit"
)

const EXPIRE_TIME = 10


// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) {
	reply.phase = c.phase
	
	reply.task_id = -1 // 如果任务doing或done,则没有任务待做
	reply.nMap = c.nMap
	reply.nReduce = c.nReduce

	if c.phase != EXIT {
		c.mtx.Lock()
		defer c.mtx.Unlock()

		for i := 0; i < c.task_num; i++ {
			if c.hasDone[i] == UNDO {
				reply.task_id = i
				c.hasDone[i] = DOING
				break
			}
		}
		
		if reply.task_id != -1 {
			go func(p jobPhase, task_id int) {
				tChannel := time.After(EXPIRE_TIME * time.Second) // 其内部其实是生成了一个Timer对象
				select {
					case <-tChannel: {
						c.mtx.Lock()
						defer c.mtx.Unlock()
						// 任务失败
						if p == c.phase && c.hasDone[task_id] == DOING {
							c.hasDone[task_id] = UNDO
						}
					}
				}
			}(c.phase, reply.task_id)
		}
	}
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	task_id := args.task_id 

	if c.hasDone[task_id] == DONE {
		log.Fatal("task %d should be doing or undo but is %d", task_id, c.hasDone[task_id])
	} else if c.hasDone[task_id] == UNDO {
		return
	}

	c.hasDone[task_id] = DONE
	c.hasDoneNum++
	if c.phase == MAP && c.hasDoneNum == c.nMap {
		c.phase = REDUCE
		c.hasDoneNum = 0
		c.task_num = c.nReduce
	} else if c.phase == REDUCE && c.hasDoneNum == c.nReduce {
		c.phase = EXIT
		c.hasDoneNum = 0
	} 
}
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
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.phase == EXIT {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.task_num = c.nMap
	c.files = files
	c.nReduce = nReduce
	c.phase = MAP
	c.hasDoneNum = 0

	len := c.nReduce
	if c.nMap > c.nReduce {
		len = c.nMap
	}
	c.hasDone = make([]taskStatus, len)

	c.server()
	return &c
}
