package core

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	id        int
	file      string
	done      bool
	startTime time.Time
}

type ReduceTask struct {
	id        int
	files     []string
	done      bool
	startTime time.Time
}
type Master struct {
	// Your definitions here.
	mu                   sync.Mutex
	MapTasks             []MapTask
	ReduceTasks          []ReduceTask
	remainingMapTasks    int
	remainingReduceTasks int
	NoOfReduceTasks      int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTaskHandler(args *FinishedTaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.Type {
	case Map:
		if !m.MapTasks[args.Id].done {
			m.MapTasks[args.Id].done = true
			for idx, file := range args.Files {
				if len(file) > 0 {
					m.ReduceTasks[idx].files = append(m.ReduceTasks[idx].files, file)
				}
			}

			m.remainingMapTasks--
		}
	case Reduce:
		if !m.ReduceTasks[args.Id].done {
			m.ReduceTasks[args.Id].done = true
			m.remainingReduceTasks--

		}
	}
	now := time.Now()
	tenSecAgo := now.Add(-10 * time.Second)
	if m.remainingMapTasks > 0 {
		for id := range m.MapTasks {
			task := &m.MapTasks[id]
			if !task.done || task.startTime.Before(tenSecAgo) {
				reply.Id = task.id
				reply.Type = Map
				reply.Files = []string{task.file}
				reply.NoOfReduceTasks = m.NoOfReduceTasks
				task.startTime = now
				return nil
			}
		}
		reply.Type = Sleep
	} else if m.remainingReduceTasks > 0 {

		for id := range m.ReduceTasks {
			task := &m.ReduceTasks[id]
			if !task.done || task.startTime.Before(tenSecAgo) {
				reply.Id = task.id
				reply.Type = Reduce
				reply.Files = task.files
				task.startTime = now
				return nil
			}
		}
		reply.Type = Sleep
	} else {
		reply.Type = Exit
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Your code here.
	return m.remainingReduceTasks == 0 && m.remainingMapTasks == 0
}

// Create Master
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		MapTasks:             make([]MapTask, len(files)),
		ReduceTasks:          make([]ReduceTask, nReduce),
		remainingMapTasks:    len(files),
		remainingReduceTasks: nReduce,
		NoOfReduceTasks:      nReduce,
	}

	for i, file := range files {
		m.MapTasks[i] = MapTask{id: i, done: false, file: file}

	}

	for i := 0; i < nReduce; i++ {
		m.ReduceTasks[i] = ReduceTask{id: i, done: false}
	}

	// Your code here.
	m.server()
	return &m
}
