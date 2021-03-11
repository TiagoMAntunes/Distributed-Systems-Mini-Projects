package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type RegisterRequest struct {
}

type RegisterReply struct {
	ID           int
	ReduceNumber int
}

type WorkRequest struct {
	ID int
}

type WorkReply struct {
	Task Job
	Wait bool
}

type WorkSubmitRequest struct {
	ID       int
	Filename string
}

type WorkSubmitReply struct {
}

const (
	MAP    = iota
	REDUCE = iota
)

type Job struct {
	JobType  int // MAP or REDUCE
	Filename string
	ID       int
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
