package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		args := true
		reply := Task{}
		success := call("Master.GetWork", &args, &reply)

		if !success || reply.Index == -1 {
			os.Exit(0)
		}
		if reply.IsMapTask {
			if !doMap(reply, mapf) {
				fmt.Println("Map Task failed %v\n", reply.Index)
			}
		} else {
			if !doReduce(reply, reducef) {
				fmt.Println("Reduce Task failed %v\n", reply.Index)
			}
		}
	}
}

func doMap(task Task, mapf func(string, string) []KeyValue) bool {
	content := readFile(task.File)
	intermediate := mapf(task.File, string(content))

	// creating #NReduce files to store output of Map function
	ofiles := make(map[int]*os.File)
	for i := 0; i < task.NReduce; i++ {
		tname := fmt.Sprintf("mr-out-%d-%d", task.Index, i)
		ofile, err := os.Create(tname)
		if err != nil {
			log.Fatalf("Error opening file %v: %v\n", tname, err)
		}
		ofiles[i] = ofile
	}

	// writing output of map to correct file based on hash of key
	for _, kv := range intermediate {
		reduceTaskNo := ihash(kv.Key) % task.NReduce
		ofile := ofiles[reduceTaskNo]
		enc := json.NewEncoder(ofile)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Error encoding file: %v", err)
		}
	}

	// closing the files
	for _, ofile := range ofiles {
		err := ofile.Close()
		if err != nil {
			log.Fatalf("Error closing file %v: %v\n", ofile.Name(), err)
		}
	}

	reply := false
	if call("Master.MapDone", &task, &reply) {
		return reply
	}
	return false
}

func doReduce(task Task, reducef func(string, []string) string) bool {
	oname := fmt.Sprintf("mr-out-%v", task.Index)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Error opening file %v: %v\n", oname, err)
	}

	// getting list of files for current reduce task
	filePattern := fmt.Sprintf("mr-out-*-%v", task.Index)
	files, err := filepath.Glob(filePattern)
	if err != nil {
		fmt.Printf("Error finding files with pattern %v: %v", filePattern, err)
		return false
	}

	// reading output of Map task and appending to intermediate
	intermediate := []KeyValue{}
	for _, filename := range files {
		content := readFile(filename)
		dec := json.NewDecoder(bytes.NewReader(content))
		for dec.More() {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				fmt.Printf("Error decoding file %v kv %v: %v", filename, kv, err)
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sorting and performing Reduce task on similar keys and storing in correct file
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	err = ofile.Close()
	if err != nil {
		log.Fatalf("Error closing file %v: %v\n", oname, err)
	}

	// deleting intermediate files created by Map task
	for _, filename := range files {
		err = os.Remove(filename)
		if err != nil {
			log.Fatalf("Error removing file %v: %v\n", filename, err)
		}
	}

	reply := false
	if call("Master.ReduceDone", &task, &reply) {
		return reply
	}
	return false
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	file.Seek(0, 0)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", filename)
	}
	return content
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
