package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	FileName string
	IdMap    int
	IdReduce int
}

type Coordinator struct {
	// Your definitions here.
	State          int32 //0 - map  1 - reduce  2- fin
	NumMapTask     int
	NumReduceTask  int
	MapTask        chan Task
	ReduceTask     chan Task
	MapTaskTime    sync.Map
	ReduceTaskTime sync.Map //da biao ji
	files          []string
}

type TimeStamp struct {
	Time int64
	Fin  bool
}

func lenSyncMap(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}

func lenTaskFin(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		if v.(TimeStamp).Fin {
			i++
		}
		return true
	})
	return i
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	state := atomic.LoadInt32(&c.State)
	if state == 0 {
		if len(c.MapTask) != 0 {
			maptask, ok := <-c.MapTask
			if ok {
				reply.XTask = maptask
			}
			reply.CurNumMapTask = len(c.MapTask)
		} else {
			reply.CurNumMapTask = -1
		}
	} else if state == 1 {
		if len(c.ReduceTask) != 0 {
			reducetask, ok := <-c.ReduceTask
			if ok {
				reply.XTask = reducetask
			}
			reply.CurNumMapTask = -1
			reply.CurNumReduceTask = len(c.ReduceTask)
		} else {
			reply.CurNumMapTask = -1
			reply.CurNumReduceTask = -1
		}
	}

	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask

	reply.State = state
	return nil
}

func (c *Coordinator) TaskFin(args *Task, reply *ExampleReply) error { //because xiancheng bu neng kua yu zhi xing
	time_now := time.Now().Unix()
	if lenTaskFin(&c.MapTaskTime) != c.NumMapTask {
		//fmt.Println(">>>>>>>>>", len(c.MapTask))
		start_time, _ := c.MapTaskTime.Load(args.IdMap)
		//c.MapTaskFin <- true
		if time_now-start_time.(TimeStamp).Time > 10 {
			return nil
		}
		c.MapTaskTime.Store(args.IdMap, TimeStamp{time_now, true})
		if lenTaskFin(&c.MapTaskTime) == c.NumMapTask {
			atomic.StoreInt32(&c.State, 1)
			for i := 0; i < c.NumReduceTask; i++ {
				c.ReduceTask <- Task{IdReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
			//c.State = 1
		}
	} else if lenTaskFin(&c.ReduceTaskTime) != c.NumReduceTask {
		//c.ReduceTaskFin <- true
		start_time, _ := c.MapTaskTime.Load(args.IdMap)
		if time_now-start_time.(TimeStamp).Time > 10 {
			return nil
		}
		c.ReduceTaskTime.Store(args.IdReduce, TimeStamp{time_now, true})
		if lenTaskFin(&c.ReduceTaskTime) == c.NumReduceTask {
			atomic.StoreInt32(&c.State, 2)
			//c.State = 2
		}
	}
	return nil
}

/*func (c *Coordinator) TimeTick() {
	state := atomic.LoadInt32(&c.State)
	time_now := time.Now().Unix()

	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {
			tmp, _ := c.MapTaskTime.Load(i)
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("map time out!")
				c.MapTask <- Task{FileName: c.files[i], IdMap: i}
				c.MapTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if state == 1 {
		for i := 0; i < c.NumReduceTask; i++ {
			tmp, _ := c.ReduceTaskTime.Load(i)
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("reduce time out!")
				c.ReduceTask <- Task{FileName: c.files[i], IdReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	}
}*/

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	state := atomic.LoadInt32(&c.State)
	time_now := time.Now().Unix()

	if state == 0 {
		for i := 0; i < c.NumMapTask; i++ {
			tmp, _ := c.MapTaskTime.Load(i)
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("map time out!")
				c.MapTask <- Task{FileName: c.files[i], IdMap: i}
				c.MapTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	} else if state == 1 {
		for i := 0; i < c.NumReduceTask; i++ {
			tmp, _ := c.ReduceTaskTime.Load(i)
			if !tmp.(TimeStamp).Fin && time_now-tmp.(TimeStamp).Time > 10 {
				fmt.Println("reduce time out!")
				c.ReduceTask <- Task{FileName: c.files[i], IdReduce: i}
				c.ReduceTaskTime.Store(i, TimeStamp{time_now, false})
			}
		}
	}
	ret := false

	if lenTaskFin(&c.ReduceTaskTime) == c.NumReduceTask {
		ret = true
	}

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:         0,
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
		files:         files,
	}

	// Your code here.
	/*for id, file := range files {
		c.MapTask <- Task{FileName: file, IdMap: id}

	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTask <- Task{IdReduce: i}
	}*/
	time_now := time.Now().Unix()
	for id, file := range files {
		c.MapTask <- Task{FileName: file, IdMap: id}
		c.MapTaskTime.Store(id, TimeStamp{time_now, false})
	}

	c.server()
	return &c
}
