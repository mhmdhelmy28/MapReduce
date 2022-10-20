package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapF func(string, string) []KeyValue,
	// Your worker implementation here.
	reduceF func(string, []string) string) {
	newTask := TaskReply{}
	finishedTask := FinishedTaskArgs{Type: None}
	for {
		newTask = GetTask(&finishedTask)
		switch newTask.Type {
		case Map:
			file := newTask.Files[0]
			intermediateOutputs := make([]KeyValue, 0)
			f, err := os.Open(file)
			defer f.Close()
			if err != nil {
				log.Fatalf("could not open file %s", file)
			}
			info, err := f.Stat()
			if err != nil {
				log.Fatalf("could not get file %s info", file)
			}

			contents := make([]byte, info.Size())
			f.Read(contents)

			kv := mapF(file, string(contents))
			intermediateOutputs = append(intermediateOutputs, kv...)
			toReduce := make(map[int][]KeyValue, 0)
			for _, kv := range intermediateOutputs {
				idx := ihash(kv.Key) % newTask.NoOfReduceTasks
				toReduce[idx] = append(toReduce[idx], kv)
			}

			reduceFiles := make([]string, newTask.NoOfReduceTasks)

			for id, kvs := range toReduce {
				fileName := fmt.Sprintf("mr-%d-%d", newTask.Id, id)
				file, err := os.Create(fileName)
				if err != nil {
					log.Fatalf("could not open file %s", fileName)
				}
				defer file.Close()
				encoder := json.NewEncoder(file)
				for _, kv := range kvs {
					err := encoder.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				reduceFiles[id] = fileName
			}
			finishedTask.Type = Map
			finishedTask.Id = newTask.Id
			finishedTask.Files = reduceFiles
		case Reduce:
			kvsToReduce := make([]KeyValue, 0)
			for _, file := range newTask.Files {
				f, err := os.Open(file)
				if err != nil {
					log.Fatalf("could not open file %s", file)
				}
				decoder := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					kvsToReduce = append(kvsToReduce, kv)
				}
			}
			sort.Sort(ByKey(kvsToReduce))
			outputFileName := "mr-out"
			outFile, err := os.Create(outputFileName)
			defer outFile.Close()
			if err != nil {
				log.Printf("could not create output file")
			}
			i := 0
			for i < len(kvsToReduce) {
				j := i + 1

				for j < len(kvsToReduce) && kvsToReduce[i].Key == kvsToReduce[j].Key {
					j++
				}
				values := make([]string, 0)
				for k := i; k < j; k++ {
					values = append(values, kvsToReduce[k].Value)
				}
				output := reduceF(kvsToReduce[i].Key, values)
				fmt.Fprintf(outFile, "%v %v\n", kvsToReduce[i].Key, output)

				i = j
			}
			finishedTask.Type = Reduce
			finishedTask.Id = newTask.Id
		case Sleep:
			time.Sleep(1 * time.Second)
			finishedTask.Type = None
		case Exit:
			return
		default:
			panic("Unknown type")
		}
	}

}

func GetTask(args *FinishedTaskArgs) TaskReply {
	reply := TaskReply{}
	ok := call("Master.GetTaskHandler", args, &reply)
	if !ok {
		fmt.Printf("could not call master")
		os.Exit(0)
	}
	return reply
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
