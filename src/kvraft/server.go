package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	CliendId int64
	ReqId    int64
	Key      string
	Value    string
	Index    int
	Term     int
}

const TYPE_Get = "Get"
const TYPE_Put = "Put"
const TYPE_Append = "Append"

type OpContext struct {
	op          *Op
	committed   chan byte
	wrongLeader bool //index位置的条目的term不一样，即leader已改变
	ignored     bool //reqid过期，导致该条目被跳过
	//保存Get结果
	keyExist bool
	value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore    map[string]string  //Key,Value
	contextmap map[int]*OpContext //Index,OpContext
	reqmap     map[int64]int64    //Conlient,ReqId

	lastAppliedIndex int //已应用到kvStore的条目的index
}

func newOpContext(op *Op) (opContext *OpContext) {
	opContext = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK
	op := &Op{
		Type:     TYPE_Get,
		CliendId: args.ClientId,
		ReqId:    args.ReqId,
		Key:      args.Key,
	}
	//写入raft
	var isleader bool
	op.Index, op.Term, isleader = kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	opCtx := newOpContext(op)
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kv.contextmap[op.Index] = opCtx
	}()

	//RPC结束前清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.contextmap[op.Index]; ok {
			if one == opCtx { //现存的上下文与之前的上下文相比较，如果相同则删除
				delete(kv.contextmap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2 * time.Second) //最大延迟2秒
	defer timer.Stop()
	select {
	case <-opCtx.committed: //如果提交了（channel里有值）,提交后opCtx中的参数值会改变
		if opCtx.wrongLeader { //index
			reply.Err = ErrWrongLeader
		} else if !opCtx.keyExist {
			reply.Err = ErrNoKey
		} else {
			reply.Value = opCtx.value
		}
	case <-timer.C: //如果两秒没提交成功，客户端重试
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK
	op := &Op{
		Type:     args.Op,
		CliendId: args.ClientId,
		ReqId:    args.ReqId,
		Key:      args.Key,
		Value:    args.Value,
	}
	var isleader bool
	op.Index, op.Term, isleader = kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	opCtx := newOpContext(op)
	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.contextmap[op.Index] = opCtx
	}()
	//RPC结束前清理上下文
	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.contextmap[op.Index]; ok {
			if one == opCtx {
				delete(kv.contextmap, op.Index)
			}
		}
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-opCtx.committed:
		if opCtx.wrongLeader {
			reply.Err = ErrWrongLeader
		} else if opCtx.ignored {
			// 说明reqid过期了，该请求被忽略，只需要告知客户端OK跳过即可
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader //超时，客户端重试
	}
}

func (kv *KVServer) startApply() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			//如果是应用快照
			if !msg.CommandValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if len(msg.Snapshot) == 0 {
						kv.kvStore = make(map[string]string)
						kv.reqmap = make(map[int64]int64)
					} else {
						//反序列化快照，安装到内存
						r := bytes.NewBuffer(msg.Snapshot) //解压日志
						d := labgob.NewDecoder(r)
						d.Decode(&kv.kvStore)
						d.Decode(&kv.reqmap)
					}
					//更新最后应用的索引
					kv.lastAppliedIndex = msg.SnapLastIndex
					DPrintf("KVServer[%d] installSnapshot, kvStore[%v], seqMap[%v] lastAppliedIndex[%v]", kv.me, len(kv.kvStore), len(kv.reqmap), kv.lastAppliedIndex)
				}()
			} else { //不是快照，应用普通log
				commend := msg.Command
				index := msg.CommandIndex
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					//更新最后应用的索引
					kv.lastAppliedIndex = index
					op := commend.(*Op) //将在通道里接收到的日志条目的commend(接口类型)转化为op类型
					opCtx, existctx := kv.contextmap[index]
					prereq, existreq := kv.reqmap[op.CliendId]
					kv.reqmap[op.CliendId] = op.ReqId

					if existctx { //存在等待结果的RPC，判断以前的状态是否和现在的一致（利用Term）
						if opCtx.op.Term != op.Term {
							opCtx.wrongLeader = true
						}
					}
					//只处理ID单调递增的客户端请求
					if op.Type == TYPE_Put || op.Type == TYPE_Append {
						if !existreq || op.ReqId > prereq { //如果该客户端没有发送过请求，或者请求是递增的，可以处理
							if op.Type == TYPE_Put {
								kv.kvStore[op.Key] = op.Value
							}
							if op.Type == TYPE_Append {
								if val, exist := kv.kvStore[op.Key]; exist {
									kv.kvStore[op.Key] = val + op.Value
								} else {
									kv.kvStore[op.Key] = op.Value
								}
							}
						} else if existctx { //如果请求序列号存在且不是递增的，并且op的上下文也存在，我们则认为reqid已过期
							opCtx.ignored = true
						}
					} else { //op.Type==Get
						if existctx {
							opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
						}
					}
					DPrintf("KVServer[%d] applyLoop, kvStore[%v]", kv.me, len(kv.kvStore))
					//唤醒挂起的RPC
					if existctx {
						close(opCtx.committed)
					}
				}()
			}
		}
	}
}

func (kv *KVServer) startsnapshot() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)
		var snapshot []byte
		var snapLastIndex int
		//锁内dump snapshot
		func() {
			//判断是否需要压缩日志
			if kv.maxraftstate != -1 && kv.rf.Compactlog(kv.maxraftstate) { //调用Compactlog不能加锁，不然会造成死锁
				//锁内压缩快照，离开锁通知raft处理
				kv.mu.Lock()
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvStore)
				e.Encode(kv.reqmap)  //当前客户端最大请求编号
				snapshot = w.Bytes() //压缩待处理的日志
				snapLastIndex = kv.lastAppliedIndex
				kv.mu.Unlock()
			}
		}()
		//锁外通知raft层截断，不然会造成死锁
		if snapshot != nil {
			//通知raft层压缩日志，压缩的都是已提交的日志
			kv.rf.Logtosnapshot(snapshot, snapLastIndex)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{}) ///必须用引用

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.contextmap = make(map[int]*OpContext)
	kv.reqmap = make(map[int64]int64)

	DPrintf("KVServer[%d] KVServer starts all Loops, maxraftstate[%d]", kv.me, kv.maxraftstate)
	go kv.startApply()
	go kv.startsnapshot()
	return kv
}
