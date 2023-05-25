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
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := TaskRequest{}
		reply := TaskResponse{}

		CallGetTask(&args, &reply)
		//time.Sleep(time.Second)

		CurNumMapTask := reply.CurNumMapTask
		CurNumReduceTask := reply.CurNumReduceTask
		State := reply.State
		if CurNumMapTask >= 0 && State == 0 {
			filename := reply.XTask.FileName
			id := strconv.Itoa(reply.XTask.IdMap)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open mapTask %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			num_reduce := reply.NumReduceTask
			bucket := make([][]KeyValue, num_reduce)
			for _, kv := range kva {
				num := ihash(kv.Key) % num_reduce
				bucket[num] = append(bucket[num], kv)
			}
			for i := 0; i < num_reduce; i++ {
				tmp_file, error := ioutil.TempFile("", "mr-map-*")
				if error != nil {
					log.Fatalf("cannot open tmp_file")
				}
				enc := json.NewEncoder(tmp_file)
				err := enc.Encode(bucket[i])
				if err != nil {
					log.Fatalf("encode bucket error")
				}
				tmp_file.Close()
				out_file := "mr-" + id + "-" + strconv.Itoa(i)
				os.Rename(tmp_file.Name(), out_file)
			}
			CallTaskFin(&reply.XTask)
		} else if CurNumReduceTask >= 0 && State == 1 {
			num_map := reply.NumMapTask
			id := strconv.Itoa(reply.XTask.IdReduce)

			intermediate := []KeyValue{}
			for i := 0; i < num_map; i++ {
				map_filename := "mr-" + strconv.Itoa(i) + "-" + id
				inputFile, err := os.OpenFile(map_filename, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reducetask %v", map_filename)
				}
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}

			sort.Sort(ByKey(intermediate))
			out_file := "mr-out-" + id
			tmp_file, err := ioutil.TempFile("", "mr-reduce-*")
			if err != nil {
				log.Fatalf("cannot open tmp_file")
			}
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmp_file, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			tmp_file.Close()
			os.Rename(tmp_file.Name(), out_file)

			CallTaskFin(&reply.XTask)
		} else if State == 2 {
			break
		} else {
			//break
			continue
		}
		time.Sleep(time.Second)
	}

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

func CallGetTask(args *TaskRequest, reply *TaskResponse) {

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("call get task ok!!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallTaskFin(args *Task) {

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	//args := ExampleArgs{}
	// declare a reply structure.
	reply := ExampleReply{}
	ok := call("Coordinator.TaskFin", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("call task fin ok!!\n")
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
