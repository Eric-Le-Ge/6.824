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

type TaskType string

const(
	Map TaskType = "Map"
	Reduce = "Reduce"
	Exit = "Exit"
)

type AssignmentArgs struct { }

type AssignmentReply struct {
	// Type of the task, one of "Map", "Reduce", "Exit"
	TaskType TaskType
	// Filename of the Map task, used when a task is Map.
	MapFile string
	// Id of the Map task, used when a task is Map.
	MapId int
	// Total amount of Map tasks, used when a task is Reduce,
	// to retrieve all intermediate files of name mr-{MapId}-{ReduceId}
	// for MapId in [0, MapCount).
	MapCount int
	// Id of the Reduce task, used when a task is Reduce.
	ReduceId int
	// Total amount of Reduce tasks, used a task is Map.
	ReduceCount int
}

type ReportCompleteArgs struct {
	// Type of the task, one of "Map", "Reduce"
	TaskType TaskType
	// Id of the Task
	TaskId int
}

type ReportCompleteReply struct { }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
