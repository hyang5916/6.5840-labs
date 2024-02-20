package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GiveTaskArgs struct {

}

type GiveTaskReply struct {
	FileName string
	FileNameList []string // Only if reduce
	TaskID int
	TaskType string // Either "map" or "reduce" or "done"
	NReduce int
}

type CompleteTaskArgs struct {
	FileName string
	TaskID int
	IntermediateFiles map[int][]string
}

type CompleteTaskReply struct {

}

type ReduceTaskFileArgs struct {
	FileName string
	TaskID int
}

type ReduceTaskFileReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
