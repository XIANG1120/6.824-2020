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

type Distributemapreq struct {
}

type Distributemapreps struct {
	Mptask *Maptask
}

type Distributereducereq struct {
}

type Distributereducereqs struct {
	Rdtask *Reducetask
}

type Finishedrequest struct {
	Mptask *Maptask
	Rdtask *Reducetask
}

type Finishedresponse struct {
	Isfinished bool
}

type Searchmrequest struct {
}

type Searchmreponse struct {
	Mapdone bool
	Alldone bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
