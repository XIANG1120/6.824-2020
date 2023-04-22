package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mux        sync.Mutex
	Mapsum     int
	Reducesum  int
	Mptask     []*Maptask
	Rdtask     []*Reducetask
	Mapdone    bool
	Reducedone bool
	Alldone    bool
}

type Maptask struct {
	Id       int
	Filename string
	St       int //0 1 2
	Nreduce  int
	timeout  *time.Timer
}

type Reducetask struct {
	Id      int
	St      int //0 1 2
	Nmap    int
	timeout *time.Timer
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Distributemap(req *Distributemapreq, reply *Distributemapreps) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	for i := 0; i < m.Mapsum; i++ {
		if m.Mptask[i].St == 0 {
			m.Mptask[i].St = 1
			m.Mptask[i].timeout = time.AfterFunc(10*time.Second, func(index int) func() {
				return func() {
					m.mux.Lock()
					defer m.mux.Unlock()
					if m.Mptask[index].St == 1 {
						m.Mptask[index].St = 0
					}
				}
			}(i))
			reply.Mptask = m.Mptask[i]
			return nil
		}
	}
	return nil
}

func (m *Master) Distributereduce(req *Distributereducereq, reply *Distributereducereqs) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	for i := 0; i < m.Reducesum; i++ {
		if m.Rdtask[i].St == 0 {
			m.Rdtask[i].St = 1
			m.Rdtask[i].timeout = time.AfterFunc(10*time.Second, func(index int) func() {
				return func() {
					m.mux.Lock()
					defer m.mux.Unlock()
					if m.Rdtask[index].St == 1 {
						m.Rdtask[index].St = 0
					}
				}
			}(i))
			reply.Rdtask = m.Rdtask[i]
			return nil
		}
	}
	return nil
}

func (m *Master) Finished(request *Finishedrequest, response *Finishedresponse) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	if request.Mptask != nil {
		for i := 0; i < m.Mapsum; i++ {
			if m.Mptask[i].Id == request.Mptask.Id && m.Mptask[i].St == 1 {
				m.Mptask[i].St = 2
				m.Mptask[i].timeout.Stop()
				response.Isfinished = true
			}
		}
	}
	flag1 := true
	for i := 0; i < m.Mapsum; i++ {
		if m.Mptask[i].St != 2 {
			flag1 = false
		}
	}
	if flag1 == true {
		m.Mapdone = true
	}
	if request.Rdtask != nil {
		for i := 0; i < m.Reducesum; i++ {
			if m.Rdtask[i].Id == request.Rdtask.Id && m.Rdtask[i].St == 1 {
				m.Rdtask[i].St = 2
				m.Rdtask[i].timeout.Stop()
				response.Isfinished = true
			}
		}
	}
	flag2 := true
	for i := 0; i < m.Reducesum; i++ {
		if m.Rdtask[i].St != 2 {
			flag2 = false
		}
	}
	if flag2 == true {
		m.Reducedone = true
	}
	if m.Reducedone == true && m.Mapdone == true {
		m.Alldone = true
	}

	return nil
}

func (m *Master) Searchm(request *Searchmrequest, response *Searchmreponse) error {
	response.Mapdone = m.Mapdone
	response.Alldone = m.Alldone
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
	ret := false
	if m.Alldone == true {
		ret = true
	}
	// Your code here.
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Mapsum:     0,
		Reducesum:  nReduce,
		Mapdone:    false,
		Reducedone: false,
		Alldone:    false,
	}

	m.Mptask = make([]*Maptask, 0)
	m.Rdtask = make([]*Reducetask, 0)
	for i, filename := range files {
		mptask := &Maptask{
			Filename: filename,
			St:       0,
			Id:       i,
			Nreduce:  nReduce,
		}
		m.Mapsum += 1
		m.Mptask = append(m.Mptask, mptask)
	}
	for i := 0; i < nReduce; i++ {
		rdtask := &Reducetask{
			Id:   i,
			St:   0,
			Nmap: m.Mapsum,
		}
		m.Rdtask = append(m.Rdtask, rdtask)
	}
	// Your code here.
	m.server()
	return &m
}
