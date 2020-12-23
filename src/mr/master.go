package mr

import  "errors"
import	"log"
import	"net"
import	"net/http"
import	"net/rpc"
import	"os"
import	"sync"
import	"time"

type TaskStatusCode string

const(
	Idle TaskStatusCode = "Idle"
	InProgress = "InProgress"
	Completed = "Completed"
)

type TaskStatus struct {
	Code TaskStatusCode
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	// Initial filename to map from.
	MapFiles []string
	// Total amount of Map tasks.
	MapCount int
	// Total amount of Reduce tasks.
	ReduceCount int
	// Statuses of Map tasks.
	MapStatus []TaskStatus
	// Statuses of Reduce tasks.
	ReduceStatus []TaskStatus
	// Boolean shorthand to indicate if all maps are completed.
	MapComplete bool
	// Boolean shorthand to indicate if all maps and reduces are completed.
	AllComplete bool
	// Condition for TaskStatus updates
	Cond *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.
//
// RPC handler for finding a task assignment.
//
func (m *Master) Assignment(args *AssignmentArgs, reply *AssignmentReply) error {
    m.Cond.L.Lock()
	for !m.MapComplete {
		// Try to find a Map task.
		for ind, mapStatus := range m.MapStatus {
			// If available task exist, assign it
			if mapStatus.Code == Idle {
				m.MapStatus[ind].Code = InProgress
				m.MapStatus[ind].StartTime = time.Now()
				m.Cond.L.Unlock()
				reply.TaskType = Map
				reply.ReduceCount = m.ReduceCount
				reply.MapId = ind
				reply.MapFile = m.MapFiles[ind]
				return nil
			}
		}
		// No available Map task found. Wait on conditional variable.
		m.Cond.Wait()
	}
	for !m.AllComplete {
		// Try to find a Reduce task.
		for ind, reduceStatus := range m.ReduceStatus {
			// If available task exist, assign it
			if reduceStatus.Code == Idle {
				m.ReduceStatus[ind].Code = InProgress
				m.ReduceStatus[ind].StartTime = time.Now()
				m.Cond.L.Unlock()
				reply.TaskType = Reduce
				reply.MapCount = m.MapCount
				reply.ReduceId = ind
				return nil
			}
		}
		// No available Reduce task found. Wait on conditional variable.
		m.Cond.Wait()
	}
	// If all tasks complete, tell worker to exit.
	m.Cond.L.Unlock()
	reply.TaskType = Exit
	return nil
}


//
// RPC handler for registering a complete task.
//
func (m *Master) ReportComplete(args *ReportCompleteArgs, reply *ReportCompleteReply) error {
	m.Cond.L.Lock()
	defer m.Cond.L.Unlock()
	if args.TaskType == Map {
		m.MapStatus[args.TaskId].Code = Completed
	} else if args.TaskType == Reduce {
		m.ReduceStatus[args.TaskId].Code = Completed
	} else {
		return errors.New("invalid task type")
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// Background controller for the map-reduce workflow. Responsible for:
// 1. Collecting timeout (10s) tasks and resetting them to Idle
// 2. Detecting if all Map or Reduce tasks are completed and setting
//    the corresponding attributes of master
// This goroutine does not have a recovery mechanism, killing it may
// cause workers to hang. A recovery mechanism can be implemented in
// mrmaster.go / master.Done() to check heartbeats and relaunch this
// goroutine if needed.
//
func (m *Master) controller() {
	m.Cond.L.Lock()
	defer m.Cond.L.Unlock()
	for !m.AllComplete {
		stageCompleted := true
		if m.MapComplete {
			for ind, reduceStatus := range m.ReduceStatus {
				// Reset overtime task to Idle
				if reduceStatus.Code == InProgress &&
					time.Now().Sub(reduceStatus.StartTime) > time.Second * 10 {
					m.ReduceStatus[ind].Code = Idle
					m.Cond.Signal()
				}
				if reduceStatus.Code != Completed {
					stageCompleted = false
				}
			}
			// Since reduce is the lsat stage, we do not need to wait another
			// second if it is completed.
			if stageCompleted {
				m.AllComplete = true
				m.Cond.Broadcast()
				break
			}
		} else {
			for ind, mapStatus := range m.MapStatus {
				// Reset overtime task to Idle
				if mapStatus.Code == InProgress &&
					time.Now().Sub(mapStatus.StartTime) > time.Second * 10 {
					m.MapStatus[ind].Code = Idle
					m.Cond.Signal()
				}
				if mapStatus.Code != Completed {
					stageCompleted = false
				}
			}
			if stageCompleted {
				m.MapComplete = true
				m.Cond.Broadcast()
			}
		}
		m.Cond.L.Unlock()
		time.Sleep(time.Second)
		m.Cond.L.Lock()
	}
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// No need to get the lock here.
	return m.AllComplete
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		MapFiles: files,
		MapCount: len(files),
		ReduceCount: nReduce,
		Cond: sync.NewCond(&sync.Mutex {}),
	}
	for i:=0; i<len(files); i++ {
		m.MapStatus = append(m.MapStatus, TaskStatus{
			Code: Idle,
		})
	}
	for i:=0; i<nReduce; i++ {
		m.ReduceStatus = append(m.ReduceStatus, TaskStatus{
			Code: Idle,
		})
	}
	go m.controller()
	m.server()
	return &m
}
