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

type Args struct {

}

type Task_Type int
const (
	Map Task_Type = 0
	Reduce Task_Type = 1
	Wait Task_Type = 2
	Exit Task_Type = 3
)
type Reply struct {
	Task_type Task_Type
	Task_id int
	File_name string
	NReduce int
	MMap int
}
type Args1 struct {
    Task_idx int
}
type Reply1 struct {
     
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
