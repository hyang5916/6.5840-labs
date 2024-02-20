package mr

import "fmt"
import "log"
import "io/ioutil"
import "os"
import "sort"
import "time"
import "net/rpc"
import "hash/fnv"
import "encoding/json"

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

	for {
		reply := CallGiveTask()
		if reply.TaskType == "done" {
			break
		}
		if len(reply.FileName) == 0 && len(reply.FileNameList) == 0{
			time.Sleep(time.Second)
			continue
		}
		if reply.TaskType == "map" {
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			defer file.Close()
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			kva := mapf(reply.FileName, string(content))
			var reduceMap = make(map[int][]KeyValue)
			for _, kv := range kva {
				reduceN := ihash(kv.Key) % reply.NReduce
				reduceMap[reduceN] = append(reduceMap[reduceN], kv)
			}
			reduceFiles := make(map[int][]string) // Maps reduceN to list of file names.
			for reduceN, kvList := range reduceMap {
				tempFile, err := os.CreateTemp("./", "mr-temp-")
				if err != nil {
					log.Fatalf("cannot open %v", reply.FileName)
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range kvList {
					enc.Encode(&kv)
				}
				os.Rename(tempFile.Name(), fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceN))
				// CallReduceTaskFile(fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceN), reduceN)
				reduceFiles[reduceN] = append(reduceFiles[reduceN], fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceN))
				tempFile.Close()
			}
			// for i := 0; i < len(kva); i += len(kva)/reply.NReduce { // Split output of map into buckets for reduce.
			// 	tempFile, err := os.CreateTemp("./", "mr-temp-")
			// 	if err != nil {
			// 		log.Fatalf("cannot open %v", reply.FileName)
			// 	}
			// 	enc := json.NewEncoder(tempFile)
			// 	for j := i; j < i + len(kva)/reply.NReduce; i++ {
			// 		enc.Encode(kva[j])
			// 	}
			// 	os.Rename(tempFile.Name(), fmt.Sprintf("mr-%d-%d", reply.TaskID, i))
			// 	CallReduceTaskFile(fmt.Sprintf("mr-%d-%d", reply.TaskID, i))
			// }
			// Send complete signal back to coordinator.
			CallCompleteTask(reply.FileName, reply.TaskID, reduceFiles)

		} else if reply.TaskType == "reduce" {
			kva := []KeyValue{}
			// fmt.Printf("file name list %v\n", reply.FileNameList)
			for _, fileName := range reply.FileNameList {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", reply.FileName)
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						// fmt.Print(err.Error())
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			tempFile, err := os.CreateTemp("./", "mr-temp-")
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}

			//
			// call Reduce on each distinct key in kva[],
			// and print the result to the tempFile.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			os.Rename(tempFile.Name(), oname)
			defaultMap := make(map[int][]string)
			CallCompleteTask(reply.FileName, reply.TaskID, defaultMap)
			tempFile.Close()
		}
		time.Sleep(time.Second)
	}

}

// RPC call to the coordinator that requests a task.
func CallGiveTask() GiveTaskReply {
	args := GiveTaskArgs{}
	reply := GiveTaskReply{}

	ok := call("Coordinator.GiveTask", &args, &reply)
	if ok {
		// fmt.Printf("reply.FileName %v\n", reply.FileName)
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return reply
	}
}

func CallCompleteTask(fileName string, taskID int, intermediateFiles map[int][]string) CompleteTaskReply {
	args := CompleteTaskArgs{}
	reply := CompleteTaskReply{}

	args.FileName = fileName
	args.TaskID = taskID
	args.IntermediateFiles = intermediateFiles

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		// fmt.Printf("task completed!\n")
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return reply
	}
}

func CallReduceTaskFile(fileName string, taskID int) ReduceTaskFileReply {
	args := ReduceTaskFileArgs{}
	reply := ReduceTaskFileReply{}

	args.FileName = fileName
	args.TaskID = taskID

	ok := call("Coordinator.ReduceTaskFile", &args, &reply)
	if ok {
		// fmt.Printf("call completed!\n")
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return reply
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
