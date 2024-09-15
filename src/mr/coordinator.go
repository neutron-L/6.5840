package mr

import "log"
import "net"
import "os"
import "sync"
import "net/rpc"
import "net/http"


type taskStatus int

type Coordinator struct {
	// Your definitions here.
	phase   	jobPhase
	workers 	map[string]bool   // 记录连接过的map以及是否发送exit
	hasDoneNum	int
	hasDone		[]taskStatus      // 任务完成情况

	mtx		sync.Mutex
}


const (
	nMap    = 100
	nReduce = 50
)

const (
	UNDO = 1
	DOING = 2
	DONE = 3
)

const EXPIRE_TIME = 10


// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) {
	reply->phase = phase
	reply->task_id = -1 // 如果任务doing或done,则没有任务待做

	if phase != EXIT {
		mtx.lock()
		defer mtx.unlock()

		task_num := nMap
		if phase == REDUCE {
			task_num = nReduce
		}
		for i := 0; i < task_num; ++i {
			if hasDone[i] == UNDO {
				reply->task_id = i
				break
			}
		}
		
		if reply->task_id != -1 {
			go func(p jobPhase, task_id int) {
				tChannel := time.After(EXPIRE_TIME * time.Second) // 其内部其实是生成了一个Timer对象
				select {
					case <-tChannel: {
						mtx.lock()
						defer mtx.unlock()
						// 任务失败
						if p == phase && hasDone[task_id] == DOING {
							hasDone[task_id] = UNDO
						}
					}
				}
			}(phase, reply->task_id)
		}
	}
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) {
	mtx.lock()
	defer mtx.unlock()
	task_id := TaskDoneArgs->task_id 
	if hasDone[task_id] == DONE {
		log.Fatal("task %d should be doing or undo but is %d", task_id, hasDone[task_id])
	} else if hasDone[task_id] == UNDO {
		return
	}

	hasDone[task_id] = DONE
	++hasDoneNum
	if phase == MAP && hasDoneNum == nMap {
		phase = REDUCE
		hasDoneNum = 0
	} else if phase == REDUCE && hasDoneNum == nReduce {
		phase = EXIT
		hasDoneNum = 0
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
	mtx.lock()
	defer mtx.unlock()
	if phase == EXIT {
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


	c.server()
	return &c
}
