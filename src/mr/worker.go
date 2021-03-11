package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

const SLEEP_TIME = 3

var nReduce int

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	id, count, ok := register()
	if !ok {
		return // exit
	}
	nReduce = count

	fmt.Println("I am id ", id)

	for {
		task, wait, ok := workRequest(id)
		if !ok {
			return // exit
		}

		// do work
		if !wait {
			work(task, mapf, reducef)

			ok := submitJob(id, task.Filename)
			if !ok {
				return // exit
			}

		} else {
			fmt.Println("Waiting...")
			time.Sleep(time.Second * 3)
		}

	}

}

func work(task Job, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// TODO real work
	if task.JobType == MAP {
		// do map
		file, err := os.Open(task.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", task.Filename)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.Filename)
		}

		file.Close()

		kva := mapf(task.Filename, string(content))

		// sort the obtained keys
		sort.Sort(ByKey(kva))

		// create temporary files
		files := make([]*os.File, nReduce)
		encoders := make([]*json.Encoder, nReduce)
		for i := 0; i < nReduce; i++ {
			name := fmt.Sprintf("mr-%v-%v*", task.ID, i)
			tmpfile, ok := ioutil.TempFile(".", name)
			if ok != nil {
				log.Fatalf("cannot open temp file %v", name)
			}

			files[i] = tmpfile
			encoders[i] = json.NewEncoder(tmpfile)
		}

		// write keys to each temporary file
		for _, kv := range kva {
			i := ihash(kv.Key) % nReduce
			encoders[i].Encode(&kv)
		}

		// close all files and rename them
		for i := 0; i < nReduce; i++ {
			oldname := files[i].Name()
			files[i].Close()

			newname := fmt.Sprintf("mr-%v-%v", task.ID, i)
			ok := os.Rename(oldname, newname)
			if ok != nil {
				log.Fatalf("cannot rename temp file %v", oldname)
			}
		}

	} else {
		// do reduce
		// TODO
	}
}

func submitJob(id int, filename string) bool {
	req := WorkSubmitRequest{ID: id, Filename: filename}
	rep := WorkSubmitReply{}

	ok := call("Coordinator.SubmitWork", &req, &rep)

	return ok
}

func workRequest(id int) (Job, bool, bool) {
	req := WorkRequest{ID: id}
	rep := WorkReply{}

	ok := call("Coordinator.GetWork", &req, &rep)

	return rep.Task, rep.Wait, ok
}

func register() (int, int, bool) {
	req := RegisterRequest{}
	rep := RegisterReply{}

	ok := call("Coordinator.Register", &req, &rep)

	return rep.ID, rep.ReduceNumber, ok
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
