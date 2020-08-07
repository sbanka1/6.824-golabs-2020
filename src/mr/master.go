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

type Master struct {
	// Your definitions here.
	mapTasks    map[int]Task
	reduceTasks map[int]Task
	timers      map[int]*time.Timer
	lock        sync.Mutex
	wg          sync.WaitGroup
}

var MapChan chan int
var ReduceChan chan int

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetWork(args *bool, reply *Task) error {
	for mapID := range MapChan {
		m.lock.Lock()
		*reply = m.mapTasks[mapID]
		m.timers[mapID] = time.AfterFunc(time.Second*10,
			func() {
				m.lock.Lock()
				MapChan <- m.mapTasks[mapID].Index
				m.lock.Unlock()
				m.wg.Done()
			})
		m.lock.Unlock()
		m.wg.Add(1)
		return nil
	}
	m.wg.Wait()
	for reduceID := range ReduceChan {
		m.lock.Lock()
		*reply = m.reduceTasks[reduceID]
		m.timers[reduceID] = time.AfterFunc(time.Second*10,
			func() {
				m.lock.Lock()
				ReduceChan <- m.reduceTasks[reduceID].Index
				m.lock.Unlock()
			})
		m.lock.Unlock()
		return nil
	}
	*reply = Task{Index: -1}
	return nil
}

func (m *Master) MapDone(args *Task, reply *bool) error {
	index := (*args).Index
	m.lock.Lock()
	m.timers[index].Stop()
	delete(m.mapTasks, index)
	if len(m.mapTasks) == 0 {
		close(MapChan)
	}
	m.lock.Unlock()
	m.wg.Done()
	*reply = true
	return nil
}

func (m *Master) ReduceDone(args *Task, reply *bool) error {
	index := (*args).Index
	m.lock.Lock()
	m.timers[index].Stop()
	delete(m.reduceTasks, index)
	if len(m.reduceTasks) == 0 {
		close(ReduceChan)
	}
	m.lock.Unlock()
	*reply = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.lock.Lock()
	result := len(m.mapTasks) == 0 && len(m.reduceTasks) == 0
	m.lock.Unlock()
	return result
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:    make(map[int]Task),
		reduceTasks: make(map[int]Task),
		timers:      make(map[int]*time.Timer),
	}

	// Your code here.
	MapChan = make(chan int, len(files))
	ReduceChan = make(chan int, nReduce)

	for i, file := range files {
		task := Task{
			IsMapTask: true,
			File:      file,
			Index:     i,
			NReduce:   nReduce,
		}
		m.mapTasks[i] = task
		MapChan <- i
	}
	for i := 0; i < 10; i++ {
		task := Task{
			IsMapTask: false,
			Index:     i,
			NReduce:   nReduce,
		}
		m.reduceTasks[i] = task
		ReduceChan <- i
	}
	// close(MapChan)
	// close(ReduceChan)

	m.server()
	return &m
}
