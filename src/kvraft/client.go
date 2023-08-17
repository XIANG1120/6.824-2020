package kvraft

import (
	"../labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId int
	ClientId int64
	ReqId    int64 //请求序列号
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	return ck
}

func (ck *Clerk) currentleaderId() int {
	ck.mu.Lock() //为啥要加锁？
	defer ck.mu.Unlock()
	leaderId := ck.LeaderId
	return leaderId
}

func (ck *Clerk) changeleaderId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
	return ck.LeaderId
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.ClientId,
		ReqId:    atomic.AddInt64(&ck.ReqId, 1),
	}
	DPrintf("Client[%d] Get, Key=%s ", ck.ClientId, key)
	LeaderId := ck.currentleaderId()
	value := ""
	for {
		reply := GetReply{}
		if ck.servers[LeaderId].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK {
				value = reply.Value
				return value
			}
			if reply.Err == ErrNoKey {
				return value
			}
		}
		LeaderId = ck.changeleaderId()
		time.Sleep(1 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.ClientId,
		ReqId:    atomic.AddInt64(&ck.ReqId, 1),
	}
	DPrintf("Client[%d] PutAppend, Key=%s Value=%s", ck.ClientId, key, value)
	LeaderId := ck.currentleaderId()
	for {
		reply := PutAppendReply{}
		if ck.servers[LeaderId].Call("KVServer.PutAppend", args, &reply) {
			if reply.Err == OK {
				break
			}
		}
		LeaderId = ck.changeleaderId()
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
