package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

func init() {
	log.SetFlags(log.Ltime | log.Ldate | log.Lshortfile)
}

//
// Map functions return a slice of KeyValue.

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

type ApplyForTaskArgs struct {
	WorkerID      string
	LastTaskType  State
	LastTaskIndex int
}

type ApplyForTaskReply struct {
	TaskType     State //有三种状态，map，reduce和""，最后的代表结束了
	TaskIndex    int
	MapNum       int
	ReduceNum    int
	MapInputFile string
}

//
// main/mrworker.go calls this function.

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)
	// 进入循环，向 Coordinator 申请 Task
	var lastTaskType State
	var lastTaskIndex int
	for {
		args := ApplyForTaskArgs{
			WorkerID:      id,
			LastTaskType:  lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}

		reply := new(ApplyForTaskReply)
		call("Coordinator.ApplyForTask", &args, &reply)
		//log.Printf("Received %+v", *reply)
		if reply.TaskType == MAP {
			// 处理 MAP Task
			// 读取输入数据
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatalf("Failed to open map input file %s: %s", reply.MapInputFile, err.Error())
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
			}
			// 传递输入数据至 MAP 函数，得到中间结果
			kva := mapf(reply.MapInputFile, string(content))
			// 按 Key 的 Hash 值对中间结果进行分桶
			hashedKva := make(map[int][]KeyValue)

			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}

			// 写出中间结果到文件
			for i := 0; i < reply.ReduceNum; i++ {
				// id是pid，reply.TaskIndex是第几个文件，i是第几个reduce
				ofile, _ := os.Create(tmpMapOutFile(reply.TaskIndex, i))
				for _, kv := range hashedKva[i] {
					fprintf, err := fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
					if err != nil {
						log.Println(fprintf)
						return
					}
				}
				ofile.Close()
			}

			call("Coordinator.CompleteTask", &reply, &args)
		} else if reply.TaskType == REDUCE {
			// 读取输入数据
			var lines []string
			for mi := 0; mi < reply.MapNum; mi++ {
				inputFile := tmpMapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open map output file %s: %s", inputFile, err.Error())
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key:   parts[0],
					Value: parts[1],
				})
			}

			// 按 Key 对输入数据进行排序
			sort.Sort(ByKey(kva))

			ofile, _ := os.Create(tmpReduceOutFile(reply.TaskIndex))

			// 按 Key 对中间结果的 Value 进行归并，传递至 Reduce 函数
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// 写出至结果文件
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
			call("Coordinator.CompleteTask", &reply, &args)
		} else if reply.TaskType == Wait {
			time.Sleep(5 * time.Second)
		} else if reply.TaskType == COMPLETE {
			return
		}
		// 记录已完成 Task 的信息，在下次 RPC 调用时捎带给 Coordinator
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
		//log.Printf("Finished %v task %d", reply.TaskType, reply.TaskIndex)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	//log.Printf("Worker %s exit\n", id)

}

func tmpMapOutFile(taskIndex int, i int) string {
	return fmt.Sprintf("mr-tmp-%d-%d", taskIndex, i)
}

func tmpReduceOutFile(taskIndex int) string {
	return fmt.Sprintf("mr-out-%d", taskIndex)
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
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Println(err)
		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
