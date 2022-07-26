package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type State int

func init() {
	log.SetFlags(log.Ltime | log.Ldate | log.Lshortfile)
}

const (
	MAP = iota
	REDUCE
	Wait
	COMPLETE
)

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex // 保护共享信息，避免并发冲突
	stage          State      // 当前作业阶段，MAP or REDUCE。为空代表已完成可退出
	nMap           int
	nReduce        int
	tasks          map[string]*Task
	availableTasks chan *Task
}

type Task struct {
	Type         State
	Index        int
	MapInputFile string
	WorkerID     string
	Deadline     time.Time
}

// GenTaskID 基于 Task 的类型和 Index 值生成唯一 ID
func GenTaskID(t State, index int) string {
	return fmt.Sprintf("%d-%d", t, index)
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Println(err)
		return
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	err = os.Remove(sockname)
	if err != nil {
		log.Println(err)
		return
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

func (c *Coordinator) Done() bool {
	c.lock.Lock()

	defer c.lock.Unlock()
	return c.stage == COMPLETE
	// Your code here.
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]*Task),
		availableTasks: make(chan *Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	// 每个输入文件生成一个 MAP Task
	log.Println(files)
	for i, file := range files {
		task := &Task{
			Type:         MAP,
			Index:        i, //第几个文件
			MapInputFile: file,
		}

		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	//log.Printf("Coordinator start\n")
	c.server()
	// 启动 Task 自动回收过程
	go func() {
		for {
			time.Sleep(5000 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				// work干完活把workid设成空
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					//log.Printf(
					//	"Found timed-out %d task %d previously running on worker %s. Prepare to re-assign",
					//	task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

// ApplyForTask RPC 的处理入口，由 Worker 调用
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	// 获取一个可用 Task 并返回
	c.lock.Lock()

	defer c.lock.Unlock()
	if len(c.availableTasks) > 0 {
		task, ok := <-c.availableTasks
		//log.Printf("Assign %d task %d to worker %s\n", task.Type, task.Index, args.WorkerID)
		if !ok { // Channel 关闭，代表整个 MR 作业已完成，通知 Worker 退出
			return nil
		}
		task.WorkerID = args.WorkerID
		task.Deadline = time.Now().Add(10 * time.Second)

		c.tasks[GenTaskID(task.Type, task.Index)] = task // 记录 Task 分配的 Worker ID 及 Deadline
		*reply = ApplyForTaskReply{
			TaskType:     task.Type,
			TaskIndex:    task.Index,
			MapNum:       c.nMap,
			ReduceNum:    c.nReduce,
			MapInputFile: task.MapInputFile,
		}
	} else if c.stage == COMPLETE {
		*reply = ApplyForTaskReply{
			TaskType: COMPLETE,
		}
	} else {
		*reply = ApplyForTaskReply{
			TaskType: Wait,
		}
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *ApplyForTaskReply, reply *ApplyForTaskArgs) error {
	c.lock.Lock()
	//log.Println(*args, GenTaskID(args.TaskType, args.TaskIndex))
	c.tasks[GenTaskID(args.TaskType, args.TaskIndex)].Type = COMPLETE
	c.tasks[GenTaskID(args.TaskType, args.TaskIndex)].WorkerID = ""
	if c.allTaskDone() {
		c.transit()
	}
	c.lock.Unlock()

	return nil
}

func (c *Coordinator) allTaskDone() bool {

	for _, task := range c.tasks {
		//log.Printf("%+v", task)
		if task.Type != COMPLETE {
			return false
		}
	}
	return true
}

func (c *Coordinator) transit() {

	if c.stage == MAP {
		// MAP Task 已全部完成，进入 REDUCE 阶段
		//log.Printf("All MAP tasks finished. Transit to REDUCE stage\n")
		c.stage = REDUCE
		// 生成 Reduce Task
		for i := 0; i < c.nReduce; i++ {
			task := &Task{
				Type:  REDUCE,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == REDUCE {
		// REDUCE Task 已全部完成，MR 作业已完成，准备退出
		//log.Printf("All REDUCE tasks finished. Prepare to exit\n")
		close(c.availableTasks) // 关闭 Channel，响应所有正在同步等待的 RPC 调用
		c.stage = COMPLETE      // 使用空字符串标记作业完成
	}
}
