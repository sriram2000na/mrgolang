package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	_ "sync"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Holder struct {
	Data []KeyValue
}

// var filesMutex sync.Mutex

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func appendToFile(filename string, contents []KeyValue) {
	// filesMutex.Lock()
	// defer filesMutex.Unlock()
	f, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	var existing Holder
	text, _ := io.ReadAll(f)
	json.Unmarshal(text, &existing)
	existing.Data = append(existing.Data, contents...)
	os.Remove(filename)
	f.Close()
	f2, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	defer f2.Close()
	toWrite, _ := json.Marshal(existing)
	// ct := strings.Count(string(toWrite), "yesterday")
	// fmt.Printf("got yday %d times\n", ct)
	fmt.Println(filename)
	f2.Write(toWrite)
}

func unwrapReduceArgs(task Task) Holder {
	files, _ := filepath.Glob(fmt.Sprintf("mr-*-%s", task.Arg_filename))
	// fmt.Printf("files: %v\n", files)
	var result Holder
	for _, filename := range files {
		var existing Holder
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		json.Unmarshal(content, &existing)
		result.Data = append(result.Data, existing.Data...)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}
	return result
}

func unwrapMapArgs(task Task) (string, string) {
	file, _ := os.Open(task.Arg_filename)
	contents, _ := io.ReadAll(file)
	return task.Arg_filename, string(contents)
}

func callWithErr(method string, inp interface{}, out interface{}) {
	ok := call(method, inp, out)
	if !ok {
		err_msg := fmt.Errorf("Couldn't call %s", method)
		fmt.Println(err_msg)
		return
	}
}

type WorkerInfo struct {
	workerId int
	nreduce  int
}

func setupWorker() WorkerInfo {
	var nreduce int
	callWithErr("Coordinator.GetR", struct{}{}, &nreduce)
	var workerId int
	callWithErr("Coordinator.GetId", struct{}{}, &workerId)
	return WorkerInfo{workerId, nreduce}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	info := setupWorker()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		// fmt.Println("Asking for task")
		reply := GetTaskReply{}
		callWithErr("Coordinator.GetTask", struct{}{}, &reply)
		// fmt.Printf("Got task?: %t\n", reply.HasTask)
		if reply.HasTask {
			processTask(reply.Task, mapf, reducef, info.workerId, info.nreduce)
		}
		time.Sleep(1 * time.Second)
	}
}

func processTask(task Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, workerId int, nreduce int) {
	// fmt.Print("task: ")
	// fmt.Println(task.Name)
	if task.Name == MAP_TASK_TYPE {
		// Call map function
		filename, contents := unwrapMapArgs(task)
		outs := mapf(filename, contents)
		fileContents := make(map[int][]KeyValue)
		for _, val := range outs {
			reduce_task := ihash(val.Key) % nreduce
			fileContents[reduce_task] = append(fileContents[reduce_task], val)
		}
		for i := 0; i < nreduce; i++ {
			outFile := fmt.Sprintf("mr-%d-%d", workerId, i)
			if len(fileContents[i]) == 0 {
				continue
			}
			appendToFile(outFile, fileContents[i])
		}
		reply := false
		// fmt.Printf("Completed map task: %#v", task)
		callWithErr("Coordinator.TaskOverNotify", task, &reply)
	} else {
		// fmt.Printf("reducing workerid: %d\n", workerId)
		intermediate := unwrapReduceArgs(task)
		sort.Sort(ByKey(intermediate.Data))
		// fmt.Println(intermediate.Data)

		i := 0
		for i < len(intermediate.Data) {
			j := i + 1
			for j < len(intermediate.Data) && intermediate.Data[j].Key == intermediate.Data[i].Key {
				j++
			}
			values := make([]string, 0)
			for k := i; k < j; k++ {
				values = append(values, intermediate.Data[k].Value)
			}
			if intermediate.Data[i].Key == "yesterday" {
				// fmt.Printf("Values: %v", values)
			}
			output := reducef(intermediate.Data[i].Key, values)
			if intermediate.Data[i].Key == "yesterday" {
				// fmt.Println("Value for yesterday: ")
				// fmt.Println(intermediate.Data[i].Value)
				// fmt.Printf("output for yesterday: %s\n", output)
				// fmt.Println(task.Arg_filename)
			}
			// this is the correct format for each line of Reduce output.
			appendOuputs(output, intermediate.Data[i].Key, fmt.Sprintf("mr-out-%s", task.Arg_filename))
			i = j
		}
		reply := false
		callWithErr("Coordinator.TaskOverNotify", task, &reply)
		if reply {
			return
		}
	}
}

func writeToFile(output, key, filename string) {
	ofile, _ := os.Create(filename)
	fmt.Fprintf(ofile, "%v %v\n", key, output)
	ofile.Close()
}

func appendOuputs(output, key, filename string) {
	f, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	contents, _ := io.ReadAll(f)
	toWrite := string(contents)
	toWrite += fmt.Sprintf("%v %v\n", key, output)
	f.WriteString(toWrite)
	f.Close()
}

func getJSONBytes(data []KeyValue) []byte {
	bytes, _ := json.Marshal(data)
	return bytes
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
