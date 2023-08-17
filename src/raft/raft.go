package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	//应用日志
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	//应用快照
	Snapshot      []byte
	SnapLastIndex int
	SnapLastTerm  int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const role_Leader = "Leader"
const role_Follower = "Follower"
const role_Candidates = "Candidates"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor    int //每当term变时，必须重置
	log         []*LogEntry

	commitIndex int    //当超过1/2的节点都已经match，则可以修改leader的commitIndex
	lastApplied int    //应用于状态机上的最后一个条目的下标
	role        string //图2中没有

	//只有leader有的两个成员
	nextIndex  []int //要复制的日志的第一个条目的下标
	matchIndex []int //只有复制成功，才会修改

	electiontime time.Time //两个计时器
	heartbeatime time.Time

	snapLastIndex int //快照的最后一个条目的下标
	snapLastTerm  int //快照的最后一个条目的任期
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int //用来更改peer的commitIndex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int //以下两个是用来辅助修改nextIndex[]的
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term          int
	Data          []byte
	SnapLastIndex int
	SnapLastTerm  int
	Done          bool
	Applychan     chan ApplyMsg
}
type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.role == role_Leader {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.Stateforpersist()
	DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v] SnapLastIndex[%d] SnapLastTerm[%d]", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.snapLastIndex, rf.snapLastTerm)
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) Stateforpersist() (data []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapLastIndex)
	e.Encode(rf.snapLastTerm)
	data = w.Bytes()
	return
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock() //读持久化内容时要加锁
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.snapLastIndex)
	d.Decode(&rf.snapLastTerm)
}

//返回raft最后一条log数据的Term
func (rf *Raft) lastTerm() (LastLogTerm int) {
	LastLogTerm = rf.snapLastTerm
	if len(rf.log) != 0 {
		LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

//返回raft最后一条log数据的Index，Index是从1开始的
func (rf *Raft) lastIndex() (Index int) {
	Index = rf.snapLastIndex + len(rf.log)
	return
}

//将全局index i转换为log下标
func (rf *Raft) indextolog(i int) (index int) {
	index = i - rf.snapLastIndex - 1
	return
}

//判断日志是否需要压缩
func (rf *Raft) Compactlog(maxlength int) (compact bool) {
	rf.mu.Lock() //读也需要加锁
	defer rf.mu.Unlock()
	compact = false
	if maxlength <= rf.persister.RaftStateSize() {
		compact = true
	}
	return
}

//更新leader的commitIndex
func (rf *Raft) updatecommitIndex() {
	MatchIndexarr := make([]int, 0)
	MatchIndexarr = append(MatchIndexarr, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		MatchIndexarr = append(MatchIndexarr, rf.matchIndex[i])
	}
	sort.Ints(MatchIndexarr)
	newCommitIndex := MatchIndexarr[len(rf.peers)/2]
	//如果index属于snapshot范围，那么不要检查term了，因为snapshot的一定是集群提交的???
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.snapLastIndex || rf.log[rf.indextolog(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
	DPrintf("RaftNode[%d] updateCommitIndex, commitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, MatchIndexarr)
}

//向application层应用快照
func (rf *Raft) installSnapshotToApplication(apply chan ApplyMsg) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	applysnapshot := &ApplyMsg{
		CommandValid:  false,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapLastIndex: rf.snapLastIndex,
		SnapLastTerm:  rf.snapLastTerm,
	}
	rf.lastApplied = rf.snapLastIndex //为什么不加锁？
	DPrintf("RaftNode[%d] installSnapshotToApplication, snapshotSize[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, len(applysnapshot.Snapshot), applysnapshot.SnapLastIndex, applysnapshot.SnapLastTerm)
	apply <- *applysnapshot
}

//向application层应用日志
func (rf *Raft) logToApplication(apply chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		applylog := make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock() //加锁，防止和snapshot的提交交错产生bug
			defer rf.mu.Unlock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				applylog = append(applylog, ApplyMsg{
					Command:      rf.log[rf.indextolog(rf.lastApplied)].Command,
					CommandValid: true,
					CommandIndex: rf.lastApplied,
				}) //把要提交的log全收集到applylog
				DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			}
			for _, log := range applylog {
				apply <- log
			}
		}()
	}
}

//截断日志的一部分作为快照
func (rf *Raft) Logtosnapshot(snapshot []byte, lastIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIndex <= rf.snapLastIndex { //如果比现存快照小，则丢弃
		return
	}
	//fmt.Println(lastIndex)
	//	fmt.Println(rf.SnapLastIndex)

	// 快照的当前元信息
	DPrintf("RafeNode[%d] TakeSnapshot begins, snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, lastIndex, rf.snapLastIndex, rf.snapLastTerm)
	compactlength := lastIndex - rf.snapLastIndex //压缩多长
	//fmt.Println(rf.indextolog(lastIndex))
	rf.snapLastTerm = rf.log[rf.indextolog(lastIndex)].Term
	rf.snapLastIndex = lastIndex
	// 压缩日志
	afterLog := make([]*LogEntry, len(rf.log)-compactlength)
	copy(afterLog, rf.log[compactlength:])
	rf.log = afterLog

	rf.persister.SaveStateAndSnapshot(rf.Stateforpersist(), snapshot) //持久化存储
	DPrintf("RafeNode[%d] TakeSnapshot ends,  snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, lastIndex, rf.snapLastIndex, rf.snapLastTerm)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] ", rf.me, args.CandidateId, reply.VoteGranted)
	}()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = role_Follower //收到term比自己大的msg，转变身份为follower
		rf.votedFor = -1
		rf.persist()
	}
	if args.LastLogTerm < rf.lastTerm() || (args.LastLogTerm == rf.lastTerm()) && args.LastLogIndex < rf.lastIndex() || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//rf.votedFor == -1||rf.voteFor == args.CandidateId

	rf.votedFor = args.CandidateId
	rf.role = role_Follower //一旦投票，则角色转变为follower
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.electiontime = time.Now()

	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s]",
		rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.role)
	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s]",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.role)
	}()
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.role = role_Follower
		rf.votedFor = -1
		rf.persist()
	}
	rf.electiontime = time.Now() //收到msg，选举时间重置

	if rf.snapLastIndex > args.PrevLogIndex { //要复制的数据的前一条log的index小于rf的快照里最后一条log的index
		reply.ConflictIndex = 1
		return
	} else if rf.snapLastIndex == args.PrevLogIndex { //相等，接下来判断term
		if rf.snapLastTerm != args.PrevLogTerm {
			reply.ConflictIndex = 1
			return
		}
	} else { //rf.SnapLastIndex < args.PrevLogIndex
		if rf.lastIndex() >= args.PrevLogIndex {
			if rf.log[rf.indextolog(args.PrevLogIndex)].Term != args.PrevLogTerm {
				reply.ConflictTerm = rf.log[rf.indextolog(args.PrevLogIndex)].Term
				for index := rf.snapLastIndex + 1; index <= args.PrevLogIndex; index++ { //找到冲突的term的第一条log
					if rf.log[rf.indextolog(index)].Term == reply.ConflictTerm {
						reply.ConflictIndex = index
						break
					}
				}
				return
			}
		} else { //rf.lastIndex() < args.PrevLogIndex
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
	}

	//复制日志
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > rf.lastIndex() {
			rf.log = append(rf.log, entry)
		} else {
			if rf.log[rf.indextolog(index)].Term != entry.Term { //如果term相同，则index相同的日志条目存放的数据一定相同
				rf.log = rf.log[:rf.indextolog(index)] //数组下标<rf.indextolog(index)的都保留
				rf.log = append(rf.log, entry)
			}
		}
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit   //同步leader的commitedindex
		if rf.lastIndex() < rf.commitIndex { //存在日志缺失的情况
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RaftNode[%d] installSnapshot starts, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.snapLastIndex, rf.snapLastTerm, args.SnapLastIndex, args.SnapLastTerm, len(rf.log))

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.role = role_Follower
		rf.votedFor = -1
		rf.persist()
	}
	rf.electiontime = time.Now() //收到msg重置选举时间

	if args.SnapLastIndex <= rf.snapLastIndex { //比现存的快照短，舍弃
		return
	}
	if args.SnapLastIndex >= rf.lastIndex() { //比全部日志都长，将日志清空
		rf.log = make([]*LogEntry, 0)
	} else {
		if rf.log[rf.indextolog(args.SnapLastIndex)].Term != args.SnapLastTerm { //term不同，舍弃快照后的所有log数据
			rf.log = make([]*LogEntry, 0)
		} else { //没有意外情况，将log中和快照重叠的数据清除
			log := make([]*LogEntry, rf.lastIndex()-args.SnapLastIndex)
			copy(log, rf.log[rf.indextolog(args.SnapLastIndex)+1:]) //数组下标>=rf.indextolog(args.SnapLastIndex)+1的都保留
			rf.log = log
		}
	}
	rf.snapLastIndex = args.SnapLastIndex
	rf.snapLastTerm = args.SnapLastTerm
	rf.persister.SaveStateAndSnapshot(rf.Stateforpersist(), args.Data) //持久化raft状态和快照
	rf.installSnapshotToApplication(args.Applychan)                    //应用到状态机上
	DPrintf("RaftNode[%d] installSnapshot ends, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.snapLastIndex, rf.snapLastTerm, args.SnapLastIndex, args.SnapLastTerm, len(rf.log))
}

func (rf *Raft) startelection() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 随机化一个超时时间
			elapses := now.Sub(rf.electiontime)                               //当前时间与electiontime的差值
			// follower -> candidates
			if rf.role == role_Follower {
				if elapses >= timeout {
					rf.role = role_Candidates
					DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			// 请求vote
			if rf.role == role_Candidates && elapses >= timeout { //elapses >= timeout是为了防止一轮选举后，该raft并没有变成follower或者leader，依旧保持Candidates重复选举
				rf.electiontime = now // 重置下次选举时间

				rf.currentTerm += 1 // 发起新任期
				rf.votedFor = rf.me // 该任期投了自己
				rf.persist()

				// 请求投票req
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIndex(),
					LastLogTerm:  rf.lastTerm(),
				}

				rf.mu.Unlock() //接下来要发送RPC，开锁

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)

				// 并发RPC请求vote
				type VoteResult struct {
					peerId int
					resp   *RequestVoteReply
				}
				voteCount := 1                                          // 收到投票个数（先给自己投1票）
				finishCount := 1                                        // 收到应答个数
				voteResultChan := make(chan *VoteResult, len(rf.peers)) //实例化一个通道
				for peerId := 0; peerId < len(rf.peers); peerId++ {     //收集每一个peer的投票结果
					go func(id int) { //goroute将每一个peer的VoteResult发送到channel中
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan: //发起投票的raft从channel中取出投票信息
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 得到大多数vote后，立即离开
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock() //再把锁加上
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				// 如果角色改变了，则忽略本轮投票结果
				if rf.role != role_Candidates {
					return
				}
				// 发现了更高的任期，切回follower
				if maxTerm > rf.currentTerm {
					rf.currentTerm = maxTerm
					rf.role = role_Follower
					rf.electiontime = time.Now() //这里可以重置electiontime
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = role_Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.heartbeatime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				}
			}
		}()
	}
}

func (rf *Raft) doAppendEnrties(Id int) {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Entries = make([]*LogEntry, 0)
	args.PrevLogIndex = rf.nextIndex[Id] - 1
	//prevlog要么是快照里的最后一条数据，要么是在log中
	if args.PrevLogIndex == rf.snapLastIndex {
		args.PrevLogTerm = rf.snapLastTerm
	} else {
		args.PrevLogTerm = rf.log[rf.indextolog(args.PrevLogIndex)].Term
	}
	args.Entries = append(args.Entries, rf.log[rf.indextolog(rf.nextIndex[Id]):]...)

	DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
		rf.me, rf.currentTerm, Id, len(rf.log), rf.nextIndex[Id], rf.matchIndex[Id], len(args.Entries), rf.commitIndex)
	//
	go func() {
		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(Id, &args, &reply); ok { //发送RPC
			rf.mu.Lock() //发送完rpc，处理自己state时加锁
			defer rf.mu.Unlock()
			defer func() {
				DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, Id, len(rf.log), rf.nextIndex[Id], rf.matchIndex[Id], rf.commitIndex)
			}()
			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.role = role_Follower
				rf.electiontime = time.Now() //重置选举时间
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了????恩，有可能
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { // 同步日志成功
				//rf.nextIndex[Id] += len(args.Entries)
				rf.nextIndex[Id] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[Id] = rf.nextIndex[Id] - 1
				rf.updatecommitIndex()
			} else { //nextIndex已得到优化
				nextIndexBefore := rf.nextIndex[Id] // 仅为打印log

				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置的term不同
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > rf.snapLastIndex; index-- { // 找conflictTerm的最后一个条目
						if rf.log[rf.indextolog(index)].Term == reply.ConflictTerm {
							conflictTermIndex = index
							break
						}
					}
					if conflictTermIndex != -1 { // leader也存在冲突term的日志，则从term最后一次出现之后的日志开始尝试同步，因为leader/follower可能在该term的日志有部分相同
						rf.nextIndex[Id] = conflictTermIndex
					} else { // leader并没有term的日志，那么把follower日志中该term首次出现的位置作为尝试同步的位置，即截断follower在此term的所有日志
						rf.nextIndex[Id] = reply.ConflictIndex
					}
				} else { // follower的prevLogIndex位置没有日志 或 prevLogIndex位置的数据已被压缩进snapshot 或 prevLogIndex==snapshotLastIndex并且prevLogIndex位置的term不相等
					rf.nextIndex[Id] = reply.ConflictIndex
				}
				DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, Id, nextIndexBefore, rf.nextIndex[Id])
			}
		}
	}()
}

func (rf *Raft) doInstallSnapshot(Id int, applychan chan ApplyMsg) {
	args := InstallSnapshotArgs{
		Term:          rf.currentTerm,
		Data:          rf.persister.ReadSnapshot(),
		SnapLastTerm:  rf.snapLastTerm,
		SnapLastIndex: rf.snapLastIndex,
		Done:          true,
		Applychan:     applychan,
	}
	reply := InstallSnapshotReply{}
	go func() {
		if rf.sendInstallSnapshot(Id, &args, &reply) {
			rf.mu.Lock() //PRC发送后，加锁
			defer rf.mu.Unlock()
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = role_Follower
				rf.votedFor = -1
				rf.electiontime = time.Now()
				rf.persist()
				return
			}
			//follower安装snapshot成功，leader状态改变：
			rf.nextIndex[Id] = rf.lastIndex() + 1  //下一次同步试着从rf.lastIndex() + 1开始
			rf.matchIndex[Id] = args.SnapLastIndex //不再是nextIndex[Id]-1
			rf.updatecommitIndex()
		}
	}()
}

func (rf *Raft) startAppendEntries(applychan chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 只有leader才向外广播心跳
			if rf.role != role_Leader {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.heartbeatime) < 100*time.Millisecond {
				return
			}
			rf.heartbeatime = time.Now() //重置心跳时间

			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				// 如果nextIndex在leader的snapshot内，那么直接同步snapshot
				if rf.nextIndex[peerId] <= rf.snapLastIndex {
					rf.doInstallSnapshot(peerId, applychan)
				} else {
					rf.doAppendEnrties(peerId)
				}
			}
		}()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//向leader中添加日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != role_Leader {
		return -1, -1, false
	}
	logentry := &LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, logentry)
	index = rf.lastIndex()
	term = rf.currentTerm
	rf.persist()
	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = role_Follower
	rf.votedFor = -1
	rf.electiontime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.installSnapshotToApplication(applyCh)
	DPrintf("RaftNode[%d] Make again", rf.me)

	go rf.startelection()
	go rf.startAppendEntries(applyCh)
	go rf.logToApplication(applyCh)

	DPrintf("Raftnode[%d]启动", me)

	return rf
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
