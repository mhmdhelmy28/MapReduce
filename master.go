package mr

import (
	"log"
	"time"
)
import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	mu                   *sync.Mutex
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
		for _, task := range m.MapTasks {
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
		for _, task := range m.ReduceTasks {
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

//func (m *Master) Sequential(files []string, nReduce int, mapFunc func(filename, content string) []KeyValue, reduceFunc func(key string, values []string) string) {
//	mr := new(Master)
//	mr.reduceF = reduceFunc
//	mr.mapF = mapFunc
//	mr.NoOfReduceTasks = nReduce
//	mr.mappedFiles = files
//	intermediateOutputs := make([]KeyValue, 0)
//	for _, file := range files {
//		f, err := os.Open(file)
//		if err != nil {
//			log.Printf("could not open file %s", file)
//		}
//		info, err := f.Stat()
//		if err != nil {
//			log.Printf("could not get file %s info", file)
//		}
//
//		contents := make([]byte, info.Size())
//		f.Read(contents)
//		f.Close()
//		kv := mapFunc(file, string(contents))
//		intermediateOutputs = append(intermediateOutputs, kv...)
//	}
//
//	sort.Sort(ByKey(intermediateOutputs))
//	outputFileName := "mr-out"
//	outFile, err := os.Create(outputFileName)
//	if err != nil {
//		log.Printf("could not create output file")
//	}
//	i := 0
//	for i < len(intermediateOutputs) {
//		j := i + 1
//
//		for j < len(intermediateOutputs) && intermediateOutputs[i].Key == intermediateOutputs[j].Key {
//			j++
//		}
//		values := make([]string, 0)
//		for k := i; k <= j; k++ {
//			values = append(values, intermediateOutputs[k].Value)
//		}
//		output := reduceFunc(intermediateOutputs[i].Key, values)
//		fmt.Fprintf(outFile, "%v %v\n", intermediateOutputs[i].Key, output)
//		i = j
//	}
//	outFile.Close()
//}

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
	}

	for i, file := range files {
		m.MapTasks = append(m.MapTasks, MapTask{id: i, done: false, file: file})
	}

	for i := 0; i < nReduce; i++ {
		m.ReduceTasks = append(m.ReduceTasks, ReduceTask{id: i, done: false})
	}
	// Your code here.
	m.server()
	return &m
}
