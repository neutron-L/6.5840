package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"strconv"
	"os"
	"time"
	"encoding/json"
	"io/ioutil"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const RETRY_TIMES = 3


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}




func mapName(mapId int, reduceId int)  string {
	return "mr-inter-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId) 
}

func reduceName(reduceId int) string {
	return "mr-out-" + strconv.Itoa(reduceId)
}

func doMap(mapf func(string, string) []KeyValue, mapTaskNumber int, file string, nReduce int) bool {
	// fp, err := os.Open(file)
	// if err != nil {
	// 	return false
	// }
	content, err := ioutil.ReadFile(file)
    if err != nil {
		fmt.Printf("read file %s", file)
        return false
    }

	kv_arr := mapf(file, string(content))

	// 创建r个enc，以json格式保存键值对
	enc_arr := make([]*json.Encoder, nReduce)
	tmpfileName := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		tmpfile, err := ioutil.TempFile("", "simple")
		// file, err := os.OpenFile("mr-out-" + strconv.Itoa(mapTaskNumber) + "-" + strconv.Itoa(i), os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return false
		}
		defer tmpfile.Close()

		tmpfileName[i] = tmpfile.Name()
		enc_arr[i] = json.NewEncoder(tmpfile)
	}

	// 将每个kv对写入对应文件
	for _, kv := range kv_arr {
		err = enc_arr[ihash(kv.Key)%nReduce].Encode(&kv)
		if err != nil {
			return false
		}
	}

	// 重命名文件
	for i := 0; i < nReduce; i++ {
		os.Rename(tmpfileName[i], mapName(mapTaskNumber, i))
	}

	return true
}

func doReduce(reducef func(string, []string) string, reduceTaskNumber int, nMap int,) bool {
	// 读取中间文件，并建立key->values[]的映射关系
	dict := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		file, err := os.Open(mapName(i, reduceTaskNumber))
		if err != nil {
			return false
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			dict[kv.Key] = append(dict[kv.Key], kv.Value)
		}
	}

	// 创建mergeFile
	ofile, err := os.OpenFile(reduceName(reduceTaskNumber), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("read merge file %s\n", reduceName(reduceTaskNumber))
		return false
	}
	defer ofile.Close()

	for k, v := range dict {
		fmt.Fprintf(ofile, "%v %v\n", k, reducef(k, v))
	}

	return true
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var getTaskArgs GetTaskArgs
	var getTaskReply GetTaskReply

	var taskDoneArgs TaskDoneArgs
	var taskDoneReply TaskDoneReply

	var ret bool

	// Your worker implementation here.
	for {
		// 请求任务
		// NOTE!!!
		// 这里有一个可疑bug,某些时候(如首次)调用call成功时,且server设置了task id,但是client这边仍然不变
		getTaskReply.Task_id = 0
		ret = false
		// for i := 0; i < RETRY_TIMES && !ret; i++ {
			ret = call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
		// }
		// 重试了若干次,默认coordinator宕机或者job done
		// 或者正式通知任务做完了
		if  getTaskReply.Phase == EXIT || !ret {
			break
		}

		// fmt.Printf("%d %s %d\n", ret, getTaskReply.Phase, getTaskReply.Task_id)
		if getTaskReply.Task_id == -1 {
			time.Sleep(3 * time.Second)
			ret = false
		} else {
			if getTaskReply.Phase == MAP {
				// fmt.Printf("Map %s %d\n", getTaskReply.File, getTaskReply.Task_id)
				ret = doMap(mapf, getTaskReply.Task_id, getTaskReply.File, getTaskReply.NReduce)
			} else {
				// fmt.Printf("Reduce %d\n", getTaskReply.Task_id)
				ret = doReduce(reducef, getTaskReply.Task_id, getTaskReply.NMap)
			}
		}

		// 任务成功则通知coordinator
		if ret {
			taskDoneArgs.Task_id = getTaskReply.Task_id
			taskDoneArgs.Phase = getTaskReply.Phase
			// fmt.Printf("%s TaskDone %d\n", taskDoneArgs.Phase, getTaskReply.Task_id)
			call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
		} else {
			// fmt.Printf("%s TaskFail %d\n", getTaskReply.Phase, getTaskReply.Task_id)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
