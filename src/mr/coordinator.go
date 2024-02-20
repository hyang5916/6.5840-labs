package mr

import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	todoMapTasks       []string // Map tasks identified by their input filename.
	// todoReduceTasks    []string // Reduce tasks identified by their intermediate input filename.
	todoReduceTasks    map[int][]string // Map of reduce task ID to a list of filenames for that reduce task ID.
	pendingMapTasks    map[string]int64 // Maps file name to time started.
	pendingReduceTasks map[int]int64	// Maps reduce task ID to the time started.
	mapTaskID          int // Unique ID for each map task.
	// reduceTaskID       int // Unique ID for each reduce task.
	reduceTaskMapping  map[int][]string // Maps reduce task IDs to the list of files with that ID.
	mu      		   sync.Mutex
	nReduce			   int // The number of reduce task buckets.
}
const TimeAllowance = 10 // Give workers 10 seconds to complete tasks.

func RemoveIndex(s []string, index int) []string {
	var result []string = append(s[:index], s[index+1:]...)
	return result
}

// Handle a new Reduce file and add it to the todoReduceTasks list.
func (c *Coordinator) ReduceTaskFile(args *ReduceTaskFileArgs, reply *ReduceTaskFileReply) error {
	c.mu.Lock()

	c.todoReduceTasks[args.TaskID] = append(c.todoReduceTasks[args.TaskID], args.FileName)
	c.reduceTaskMapping[args.TaskID] = append(c.reduceTaskMapping[args.TaskID], args.FileName)
	// fmt.Printf("reduce tasks %T\n", c.todoReduceTasks)
	c.mu.Unlock()
	return nil
}

// Marks a task as complete by removing it from the pending tasks array.
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *GiveTaskReply) error {
	c.mu.Lock()
	_, isMap := c.pendingMapTasks[args.FileName]
	_, isReduce := c.pendingReduceTasks[args.TaskID]
	if isMap {
		delete(c.pendingMapTasks, args.FileName)
		for reduceN, files := range args.IntermediateFiles {
			c.todoReduceTasks[reduceN] = append(c.todoReduceTasks[reduceN], files...)
			c.reduceTaskMapping[reduceN] = append(c.reduceTaskMapping[reduceN], files...)
		}
	} else if isReduce {
		delete(c.pendingReduceTasks, args.TaskID)
	}
	c.mu.Unlock()
	return nil
}

// Your code here -- RPC handlers for the worker to call.
// Give a task in the TODO list to a worker.
func (c *Coordinator) GiveTask(args *GiveTaskArgs, reply *GiveTaskReply) error {
	c.mu.Lock()
	if len(c.todoMapTasks) > 0 {
		reply.FileName = c.todoMapTasks[0]
		reply.TaskID = c.mapTaskID
		reply.TaskType = "map"
		reply.NReduce = c.nReduce
		c.pendingMapTasks[c.todoMapTasks[0]] = time.Now().Unix()
		c.todoMapTasks = c.todoMapTasks[1:] // Remove task
		c.mapTaskID++
	} else if len(c.todoMapTasks) == 0 && len(c.pendingMapTasks) == 0 && len(c.todoReduceTasks) > 0 { // Only start reduce tasks if all map tests have finished
		for key, value := range c.todoReduceTasks {
			reply.FileNameList = value
			reply.TaskID = key
			reply.TaskType = "reduce"
			c.pendingReduceTasks[key] = time.Now().Unix()
			delete(c.todoReduceTasks, key)
			break
		}
		// reply.FileNameList = c.todoReduceTasks[0]
		// reply.TaskID = c.reduceTaskID
		// reply.TaskType = "reduce"
		// c.pendingReduceTasks[c.todoReduceTasks[0]] = time.Now().Unix()
		// c.todoReduceTasks = c.todoReduceTasks[1:]
		// c.reduceTaskID++
	} else if len(c.todoMapTasks) == 0 && len(c.pendingMapTasks) == 0 && len(c.todoReduceTasks) == 0 && len(c.pendingReduceTasks) == 0{ // No tasks left!
		reply.TaskType = "done"
	}
	c.mu.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Periodically checks if any workers have died/are taking too long and assigns task to new worker if so.
func (c *Coordinator) CheckProgress() {
	for {
		if c.Done() {
			break
		}
		c.mu.Lock()
		for fileName, timeStarted := range c.pendingMapTasks {
			if (time.Now().Unix() - timeStarted) >= TimeAllowance { // If more than TimeAllowance has passed, re-delegate task.
				delete(c.pendingMapTasks, fileName)
				c.todoMapTasks = append(c.todoMapTasks, fileName)
			}
		}
		for taskID, timeStarted := range c.pendingReduceTasks {
			if (time.Now().Unix() - timeStarted) >= TimeAllowance { // If more than TimeAllowance has passed, re-delegate task.
				delete(c.pendingReduceTasks, taskID)
				c.todoReduceTasks[taskID] = c.reduceTaskMapping[taskID]
			}
		}
		c.mu.Unlock()
		time.Sleep(2*time.Second)
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := len(c.todoMapTasks) == 0 && len(c.pendingMapTasks) == 0 && len(c.pendingReduceTasks) == 0 && len(c.todoReduceTasks) == 0

	// Your code here.
	c.mu.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.todoReduceTasks = make(map[int][]string)
	c.pendingMapTasks = make(map[string]int64)
	c.pendingReduceTasks = make(map[int]int64)
	c.mapTaskID = 0
	c.reduceTaskMapping = make(map[int][]string)
	// c.reduceTaskID = 0
	c.nReduce = nReduce

	// Your code here.
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	c.todoMapTasks = append(c.todoMapTasks, files...)
	
	go c.CheckProgress() // Spin off a thread that checks worker progress.

	c.server()
	return &c
}
