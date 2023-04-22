package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		_, done := callsearchm()
		if done {
			return
		}
		mptask := calldistributemap()
		if mptask != nil {
			domaptask(mapf, mptask)
		}
		mapdone, _ := callsearchm()
		if mapdone {
			rdtask := calldistributereduce()
			if rdtask != nil {
				doreducetask(reducef, rdtask)
			}
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

}

func calldistributemap() *Maptask {
	req := Distributemapreq{}
	reps := Distributemapreps{}
	call("Master.Distributemap", &req, &reps)
	return reps.Mptask
}

func calldistributereduce() *Reducetask {
	req := Distributereducereq{}
	reps := Distributereducereqs{}
	call("Master.Distributereduce", &req, &reps)
	return reps.Rdtask
}

func callsearchm() (bool, bool) {
	req := Searchmrequest{}
	reps := Searchmreponse{}
	call("Master.Searchm", &req, &reps)
	return reps.Mapdone, reps.Alldone
}

func callfininshed(req *Finishedrequest) {
	resp := Finishedresponse{}
	call("Master.Finished", req, &resp)
}

func domaptask(mapf func(string, string) []KeyValue, mptask *Maptask) {
	kva := ByKey{}
	filename := mptask.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva = mapf(filename, string(content))
	sort.Sort(kva)
	intermediatearr := make([][]KeyValue, mptask.Nreduce)
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		z := ihash(kva[i].Key) % mptask.Nreduce
		for k := i; k < j; k++ {
			intermediatearr[z] = append(intermediatearr[z], kva[k])
		}
		i = j
	}
	for i := 0; i < mptask.Nreduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", mptask.Id, i)
		ofile, err := os.Create(oname + ".tmp")
		if err != nil {
			log.Fatalf("cannot write %v", oname+".tmp")
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediatearr[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot jsonencode %v", oname+".tmp")
			}
		}
		ofile.Close()
		os.Rename(oname+".tmp", oname)
	}
	req := Finishedrequest{Mptask: mptask}
	callfininshed(&req)
}

func doreducetask(reducef func(string, []string) string, rdtask *Reducetask) {
	kvs := []KeyValue{}
	for i := 0; i < rdtask.Nmap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, rdtask.Id)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		ifile.Close()
	}
	sort.Sort(ByKey(kvs))
	oname := fmt.Sprintf("mr-out-%d", rdtask.Id)
	ofile, err := os.Create(oname + ".tmp")
	if err != nil {
		log.Fatalf("cannot write %v", oname+".tmp")
	}
	for z := 0; z < len(kvs); {
		j := z + 1
		for j < len(kvs) && kvs[j].Key == kvs[z].Key {
			j++
		}
		values := []string{}
		for k := z; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[z].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kvs[z].Key, output)
		z = j
	}
	ofile.Close()
	os.Rename(oname+"tmp", oname)
	req := Finishedrequest{Rdtask: rdtask}
	callfininshed(&req)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
