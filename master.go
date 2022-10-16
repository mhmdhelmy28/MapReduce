package mr

import (
	"fmt"
	"log"
	"sort"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mapF            func(filename, content string) []KeyValue
	reduceF         func(key string, values []string) string
	NoOfReduceTasks int
	mappedFiles     []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Sequential(files []string, nReduce int, mapFunc func(filename, content string) []KeyValue, reduceFunc func(key string, values []string) string) {
	mr := new(Master)
	mr.reduceF = reduceFunc
	mr.mapF = mapFunc
	mr.NoOfReduceTasks = nReduce
	mr.mappedFiles = files
	intermediateOutputs := make([]KeyValue, 0)
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Printf("could not open file %s", file)
		}
		info, err := f.Stat()
		if err != nil {
			log.Printf("could not get file %s info", file)
		}

		contents := make([]byte, info.Size())
		f.Read(contents)
		f.Close()
		kv := mapFunc(file, string(contents))
		intermediateOutputs = append(intermediateOutputs, kv...)
	}

	sort.Sort(ByKey(intermediateOutputs))
	outputFileName := "mr-out"
	outFile, err := os.Create(outputFileName)
	if err != nil {
		log.Printf("could not create output file")
	}
	i := 0
	for i < len(intermediateOutputs) {
		j := i + 1

		for j < len(intermediateOutputs) && intermediateOutputs[i].Key == intermediateOutputs[j].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k <= j; k++ {
			values = append(values, intermediateOutputs[k].Value)
		}
		output := reduceFunc(intermediateOutputs[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", intermediateOutputs[i].Key, output)
		i = j
	}
	outFile.Close()
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
	ret := false

	// Your code here.

	return ret
}

// create a Master.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	return &m
}
