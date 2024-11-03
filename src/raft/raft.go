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
	"sync"
	"sync/atomic"
	"sort"
	"time"
	"6.5840/labgob"
	"6.5840/labrpc"
	// "fmt"
	// "log"
	// "net/http"
	// _ "net/http/pprof"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing log entry
type LogEntry struct {
	Term	int
	Command interface{}
}

type Role string
 
const (  
    Follower    Role = "Follower"     // 0  
    Candidate   Role = "Candidate"    // 1  
    Leader      Role = "Leader"       // 2  
)  

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	n		  int
	Timeout	  int64
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	// Should be updated on storage before responding to RPCs
	currentTerm			int
	votedFor			int
	log					[]LogEntry

	currentRole			Role
	currentLeader		int
	votesReceived 		[]bool
	votesNumber			int

	commitIndex			int
	lastApplied			int

	nextIndex			[]int
	matchIndex			[]int

	// 传送apply Msg的chan
	applyCh				chan ApplyMsg
	applyCond			*sync.Cond

	// snapshot相关
	lastSnapshotIndex	int
	lastSnapshotTerm	int
	snapshot			[]byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currentLeader == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || 
	   d.Decode(&rf.log) != nil || d.Decode(&rf.lastSnapshotIndex) != nil || d.Decode(&rf.lastSnapshotTerm) != nil {
		Assert(false, "readPersist error\n")
	} 
	rf.commitIndex = rf.lastSnapshotIndex
	rf.lastApplied = rf.lastSnapshotIndex
	if rf.lastSnapshotIndex != 0 && rf.persister.SnapshotSize() != 0 {
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v[%v %v]: snapshot at %v, last snapindex %v\n", rf.me, rf.currentTerm, rf.currentRole, index, rf.lastSnapshotIndex)

	if index > rf.commitIndex {
		DPrintf("%v[%v %v]: snapshot at %v but commit index %v", rf.me, rf.currentTerm, rf.currentRole, index, rf.commitIndex)
		Assert(index <= rf.commitIndex, "snapshot index is greater than commit index\n")
	}
	if index <= rf.lastSnapshotIndex {
		return
	}
	rf.snapshot = snapshot
	rf.lastSnapshotTerm = rf.log[index - rf.lastSnapshotIndex].Term

	log_len := rf.logLength()
	old_log := rf.log[index - rf.lastSnapshotIndex + 1:]
	rf.log = make([]LogEntry, log_len - (index - rf.lastSnapshotIndex) + 1)
	copy(rf.log[1:], old_log[:])
	rf.lastSnapshotIndex = index

	// apply 下标
	
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term 			int // leader’s term
	VoteGranted		bool
}


type AppendEntriesArgs struct {
	Term 			int // leader’s term
	LeaderId 		int // so follower can redirect clients
	PrevLogIndex 	int // index of log entry immediately preceding
						// new ones
	PrevLogTerm 	int //term of prevLogIndex entry
	Entries			[]LogEntry // log entries to store (empty for heartbeat;
							   // may send more than one for efficiency)
	LeaderCommit 	int // leader’s commitIndex
}


type AppendEntriesReply struct {
	Term			int
	Success 		bool

	XTerm			int   // term in the conflicting entry (if any)
    XIndex			int   // index of first entry with that term (if any)
    XLen			int   // log length

	XSnapshotIndex	int
	XSnapshotTerm	int

	Ack				int   // log复制时，follower期待的下次接收的entry的下标	
}


type InstallSnapshotRequest struct {
	Term 			  	int  // leader’s term
	LeaderId 		  	int  // so follower can redirect clients
	LastIncludedIndex 	int 
	LastIncludedTerm  	int 
	Data			  	[]byte
}

type InstallSnapshotReply struct {
	Term				int
}

func (rf *Raft)resetTimeout() {
	DPrintf("%v[%v %v] reset timer", rf.me, rf.currentTerm, rf.currentRole)
	rf.Timeout = getNow() + electionTimeout()
}

// 恢复到Follower时，积累的选票需要取消
// 调用时需要确保已经拥有rf的锁
func (rf *Raft)convertToFollower() {
	rf.currentRole = Follower
	rf.currentLeader = -1
	rf.votedFor = -1

	for i, _ := range rf.votesReceived {
		rf.votesReceived[i] = false
	}
	rf.votesNumber = 0
}


// 状态转为leader
func (rf *Raft) convertToLeader() {
	rf.currentRole = Leader
	rf.currentLeader = rf.me

	// 复制日志前做初始化，正好此时持有锁
	log_len := rf.logLength()
	for follower := 0; follower < rf.n; follower++ {
		rf.nextIndex[follower] = rf.lastSnapshotIndex + log_len + 1
		rf.matchIndex[follower] = 0

		if follower == rf.me {
			rf.matchIndex[follower] = rf.lastSnapshotIndex + log_len
		}
	}
}


// 返回日志长度，论文中日志的下标从1开始，为了方便修改，将日志长度计算封装
// 调用时需要已经持有rf的锁
func (rf *Raft)logLength() int {
	return len(rf.log) - 1
}


func (rf *Raft)apply() {
	const INIT_CAP = 30
	msgArr := make([]*ApplyMsg, INIT_CAP, INIT_CAP)
	var size = INIT_CAP
	var num int
	var i int

	for !rf.killed() {
		num = 0
		i = 0
		
		rf.mu.Lock()
		for !rf.killed() && rf.lastApplied == rf.commitIndex && rf.lastApplied >= rf.lastSnapshotIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied < rf.commitIndex && rf.lastApplied >= rf.lastSnapshotIndex {
			DPrintf("%v[%v %v]: apply msg %v->%v", rf.me, rf.currentTerm, rf.currentRole, rf.lastApplied, rf.commitIndex)
			num = rf.commitIndex - rf.lastApplied
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := &ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied - rf.lastSnapshotIndex].Command, CommandIndex: rf.lastApplied}
				if i < size {
					msgArr[i] = msg
					i++
				} else {
					msgArr = append(msgArr, msg)
				}
			}
		} else {
			DPrintf("%v[%v %v]: apply %v snapidx %v", rf.me, rf.currentTerm, rf.currentRole, rf.lastApplied, rf.lastSnapshotIndex)

			msg := &ApplyMsg{SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.lastSnapshotTerm, SnapshotIndex: rf.lastSnapshotIndex}
			num = 1
			msgArr[i] = msg
			i++

			rf.lastApplied = rf.lastSnapshotIndex
		}
		

		rf.mu.Unlock()

		if num > 0 {
			for i := 0; i < num; i++ {
				rf.applyCh <- *msgArr[i]
			}
		}

		if num > size {
			size = num
		}
	}

	return
}


func (rf *Raft)firstEntryWithTerm(l int, r int, term int) int {
	for l < r {
		mid := (l + r) >> 1
		if rf.log[mid].Term < term {
			l = mid + 1
		} else {
			r = mid
		}
	}

	return l
}



func (rf *Raft)lastEntryWithTerm(l int, r int, term int) int {
	for l < r {
		mid := (l + r) >> 1
		if rf.log[mid].Term <= term {
			l = mid + 1
		} else {
			r = mid
		}
	}

	return l - 1
}

// 检查哪些entry可以提交，调用时需要已经持有rf的锁
func (rf *Raft)commitLogEntries() {
	// replicateNum := 0
	// i := rf.commitIndex + 1
	// log_len := rf.logLength()

	arr := make([]int, rf.n, rf.n)
	copy(arr, rf.matchIndex)
	sort.Ints(arr)
	// for i <= log_len + rf.lastSnapshotIndex {
	// 	replicateNum = 0
	// 	for j := 0; j < rf.n; j++ {
	// 		if rf.matchIndex[j] >= i {
	// 			replicateNum++
	// 		}
	// 	}
		
	// 	if replicateNum < rf.n / 2 + 1 {
	// 		break
	// 	}
	// 	i++
	// }
	// i--
	// DPrintf("cmtidx: %v %v %v", i, arr[rf.n - (rf.n / 2 + 1)], rf.commitIndex)
	// DPrintf("arr:  %v", arr)
	// Assert(i == arr[rf.n - (rf.n / 2 + 1)], "compute commit index failed")
	i := arr[rf.n - (rf.n / 2 + 1)]
	
	if i > rf.commitIndex {
		if rf.log[i - rf.lastSnapshotIndex].Term == rf.currentTerm {
			rf.commitIndex = i
			rf.applyCond.Signal()
			DPrintf("%v[%v %v]: update commit index %v\n", rf.me, rf.currentTerm, rf.currentRole, i)
		} else {
			DPrintf("%v[%v %v]: cannot update commit index -> %v as term is %v \n", rf.me, rf.currentTerm, rf.currentRole, i, rf.log[i - rf.lastSnapshotIndex].Term)
		}
		
	} 
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v[%v %v]: recv request vote from %v(%v), args log idx %v log term %v\n", 
		rf.me, rf.currentTerm, rf.currentRole, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	lastTerm := 0
	log_len := rf.logLength()
	if log_len > 0 {
		lastTerm = rf.log[log_len].Term
	} else if rf.lastSnapshotIndex > 0 {
		lastTerm = rf.lastSnapshotTerm
	}
	logOk := args.LastLogTerm > lastTerm || 
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= log_len + rf.lastSnapshotIndex) 

	reply.VoteGranted = false
	if rf.currentTerm == args.Term && logOk && rf.currentLeader == -1 && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.convertToFollower()
		rf.resetTimeout()
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("%v[%v %v]: vote to %v(%v)\n", rf.me, rf.currentTerm, rf.currentRole, args.CandidateId, args.Term)
	} else if !logOk {
		DPrintf("%v[%v %v]: refuse vote to %v(%v), log len %v last term %v\n", 
			rf.me, rf.currentTerm, rf.currentRole, args.CandidateId, args.Term,
			log_len + rf.lastSnapshotIndex, lastTerm)
	} else {
		DPrintf("%v[%v %v]: refuse vote to %v(%v), has vote for %v\n", rf.me, rf.currentTerm, rf.currentRole, args.CandidateId, args.Term, rf.votedFor)
	}
	reply.Term = rf.currentTerm
}

// leader复制日志到其他机器或者发送空日志作为heartbeat信息
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	needPersistence := false

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v[%v %v]: recv log from %v(%v), prev log %v(%v) Leader commit %v\n", 
		rf.me, rf.currentTerm, rf.currentRole, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	if rf.currentTerm < args.Term  {
		rf.currentTerm = args.Term
		needPersistence = true
	}

	if rf.currentTerm == args.Term {
		rf.convertToFollower()
		rf.resetTimeout()
		rf.currentLeader = args.LeaderId
	}

	log_len := rf.logLength()
	logOk := (log_len + rf.lastSnapshotIndex >= args.PrevLogIndex) && 
			(args.PrevLogIndex == rf.lastSnapshotIndex && args.PrevLogTerm == rf.lastSnapshotTerm || args.PrevLogIndex > rf.lastSnapshotIndex && rf.log[args.PrevLogIndex- rf.lastSnapshotIndex].Term == args.PrevLogTerm)

	if !logOk {
		DPrintf("%v[%v %v]: !logOk, log len = %v, prevlogidx = %v; snapterm = %v, prevlogterm = %v", rf.me, rf.currentTerm, rf.currentRole,
			log_len + rf.lastSnapshotIndex, args.PrevLogIndex, rf.lastSnapshotTerm, args.PrevLogTerm)
	}

	if rf.currentTerm == args.Term && logOk {
		// 复制log
		suffix_len := len(args.Entries)
		if suffix_len > 0 {
			DPrintf("%v[%v %v]: recv %v entries\n", rf.me, rf.currentTerm, rf.currentRole,
				suffix_len)
		}
		if suffix_len > 0 && log_len + rf.lastSnapshotIndex > args.PrevLogIndex  {
			index := log_len + rf.lastSnapshotIndex
			if args.PrevLogIndex + suffix_len < log_len + rf.lastSnapshotIndex {
				index = args.PrevLogIndex + suffix_len
			}
			DPrintf("%v[%v %v]: last two term %v(%v) %v(%v) \n", rf.me, rf.currentTerm, rf.currentRole,
				args.Entries[index - args.PrevLogIndex - 1], args.Entries[index - args.PrevLogIndex - 1].Term, rf.log[index - rf.lastSnapshotIndex], rf.log[index - rf.lastSnapshotIndex].Term)
			if args.Entries[index - args.PrevLogIndex - 1].Term != rf.log[index - rf.lastSnapshotIndex].Term {
				DPrintf("%v[%v %v]: drop %v entries", rf.me, rf.currentTerm, rf.currentRole, log_len + rf.lastSnapshotIndex - args.PrevLogIndex)
				log_len = args.PrevLogIndex - rf.lastSnapshotIndex
				rf.log = rf.log[:log_len + 1]

				needPersistence = true
			}
		}

		if args.PrevLogIndex + suffix_len > log_len + rf.lastSnapshotIndex {
			for i := log_len + rf.lastSnapshotIndex - args.PrevLogIndex; i < suffix_len; i++ {
				rf.log = append(rf.log, args.Entries[i])
			} 
			needPersistence = true
		}

		// 更新commitIndex & apply command
		if (args.LeaderCommit > rf.lastSnapshotIndex + rf.logLength()) {
			// DPrintf("local len %v cidx %v ldidx %v\n", rf.lastSnapshotIndex + rf.logLength(), rf.commitIndex, args.LeaderCommit)
			Assert(false, "commit index greater than log len\n")
		}
		DPrintf("%v[%v %v]: oldcommit %v newcommit %v log len %v suffix_len %v args.PrevLogIndex %v", rf.me, rf.currentTerm, rf.currentRole, 
			rf.commitIndex, args.LeaderCommit, rf.logLength(), suffix_len, args.PrevLogIndex)
		if rf.commitIndex < args.LeaderCommit && args.LeaderCommit <= rf.lastSnapshotIndex + rf.logLength() {
			// go rf.apply(rf.commitIndex + 1, args.LeaderCommit - rf.commitIndex, rf.log[rf.commitIndex + 1 - rf.lastSnapshotIndex : args.LeaderCommit + 1 - rf.lastSnapshotIndex])
			DPrintf("%v[%v %v]: update commit idx %v -> %v\n", rf.me, rf.currentTerm, rf.currentRole, rf.commitIndex, args.LeaderCommit)
			rf.commitIndex = args.LeaderCommit
			rf.applyCond.Signal()

			needPersistence = true
		}
		

		reply.Term = rf.currentTerm
		reply.Success = true
		reply.Ack = suffix_len + args.PrevLogIndex + 1
		DPrintf("%v[%v %v]: log_len %v ack %d\n", rf.me, rf.currentTerm, rf.currentRole, log_len + rf.lastSnapshotIndex, reply.Ack)
	} else {
		if rf.currentTerm == args.Term {
			reply.XLen = rf.lastSnapshotIndex + rf.logLength()

			if args.PrevLogIndex > rf.lastSnapshotIndex && args.PrevLogIndex <= rf.lastSnapshotIndex + rf.logLength() {
				reply.XTerm = rf.log[args.PrevLogIndex - rf.lastSnapshotIndex].Term

				// i := args.PrevLogIndex
				// for i > 1 + rf.lastSnapshotIndex && rf.log[i - rf.lastSnapshotIndex].Term == rf.log[i - 1 - rf.lastSnapshotIndex].Term  {
				// 	i--
				// }
				// reply.XIndex = i
				reply.XIndex = rf.firstEntryWithTerm(1, args.PrevLogIndex + 1 - rf.lastSnapshotIndex, reply.XTerm)
				// Assert(, "compute XIndex failed")
				// DPrintf("%v(%v) snapshot idx %v xlen %v xterm %v xindex %v myterm %v prevterm %v\n", rf.me, rf.currentTerm, rf.lastSnapshotIndex, reply.XLen, reply.XTerm, reply.XIndex, rf.log[args.PrevLogIndex - rf.lastSnapshotIndex].Term, args.PrevLogTerm)
			} else {
				reply.XSnapshotIndex = rf.lastSnapshotIndex
				reply.XSnapshotTerm = rf.lastSnapshotTerm
			}
		} else {
			DPrintf("%v[%v %v]: reject log\n", rf.me, rf.currentTerm, rf.currentRole)
		} 
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	
	if needPersistence {
		rf.persist()
	}
}



func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	
	if rf.currentTerm < args.Term  {
		rf.currentTerm = args.Term
	}

	if rf.currentTerm == args.Term {
		rf.convertToFollower()
		rf.currentLeader = args.LeaderId
		rf.currentTerm = args.Term
		rf.persist()
	}
	reply.Term = rf.currentTerm

	// 先持久化

	DPrintf("%v[%v %v]: install snapshot idx %v\n", rf.me, rf.currentTerm, rf.currentRole, args.LastIncludedIndex)

	if rf.currentTerm > args.Term || args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}

	rf.snapshot = args.Data
	log_len := rf.logLength()
	if args.LastIncludedIndex >= rf.lastSnapshotIndex + log_len {
		rf.log = make([]LogEntry, 1)
	} else if args.LastIncludedIndex - rf.lastSnapshotIndex < log_len {
		old_log := rf.log
		rf.log = make([]LogEntry, log_len - (args.LastIncludedIndex - rf.lastSnapshotIndex) + 1)
		copy(rf.log[1:], old_log[args.LastIncludedIndex - rf.lastSnapshotIndex + 1 :])
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.persist()
	rf.applyCond.Signal()
}


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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v[%v %v]: send request vote to %v...\n", rf.me, rf.currentTerm, rf.currentRole, server)

	if !ok {
		DPrintf("%v[%v %v]: send request vote to %v failed\n", rf.me, rf.currentTerm, rf.currentRole, server)
		return
	}

	// 不仅需要调用成功且得到投票，还需要判断是否是当前任期内发起的vote
	if rf.currentRole == Candidate && reply.VoteGranted && reply.Term == rf.currentTerm {
		if !rf.votesReceived[server] {
			DPrintf("%v[%v %v]: recv vote from %v, votenum = %v\n", rf.me, rf.currentTerm, rf.currentRole, server, rf.votesNumber + 1)
			rf.votesReceived[server] = true
			rf.votesNumber++
			if rf.votesNumber >= (rf.n + 1) / 2 {
				DPrintf(" %v[%v %v]: be leader\n", rf.me, rf.currentTerm, rf.currentRole)

				rf.convertToLeader()
			}
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		DPrintf("%v[%v %v]: recv vote reply %v[%v], convert to follower\n", rf.me, rf.currentTerm, rf.currentRole, server, reply.Term)
		rf.convertToFollower()
		rf.resetTimeout()
		rf.persist()
	} else if rf.currentRole != Candidate {
		DPrintf("%v[%v %v]: now is %v\n", rf.me, rf.currentTerm, rf.currentRole, rf.currentRole)
	} 
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v[%v %v]: send log to %v...\n", rf.me, rf.currentTerm, rf.currentRole, server)


	if !ok {
		DPrintf("%v[%v %v]: send log to %v failed\n", rf.me, rf.currentTerm, rf.currentRole, server)
		return
	}
	
	log_len := rf.logLength()
	if reply.Term == rf.currentTerm && rf.currentRole == Leader {
		Assert(rf.me == rf.currentLeader, "Leader is me\n")
		if reply.Success && reply.Ack > rf.matchIndex[server] {
			rf.nextIndex[server] = reply.Ack
			rf.matchIndex[server] = reply.Ack - 1
			Assert(rf.nextIndex[server] > 0 && rf.nextIndex[server] <= rf.lastSnapshotIndex + log_len + 1, "ack is wrong")
			// commit log entries
			rf.commitLogEntries()
		} else {
			// DPrintf("%v(%v) recv suggest index %v for %v\n", rf.me, rf.currentTerm, reply.SuggestIndex, follower)
			// rf.nextIndex[follower] = reply.SuggestIndex
			DPrintf("%v[%v %v]: compute %v nextindex %v\n", rf.me, rf.currentTerm, rf.currentRole, server, rf.nextIndex[server])

			if reply.XSnapshotIndex != 0 {
				rf.nextIndex[server] = reply.XSnapshotIndex + 1
			} else {
				if rf.nextIndex[server] > reply.XLen + 1 {
					rf.nextIndex[server] = reply.XLen + 1 
				} else {
					// 查找第一个大于xterm的term的下标，二分法
					

					// Assert(, "compute nextIndex failed")
					idx := rf.lastEntryWithTerm(1, rf.logLength() + 1, reply.XTerm)

					if idx == 0 {
						rf.nextIndex[server] = 1 + rf.lastSnapshotIndex
					} else {
						if rf.log[idx].Term != reply.XTerm {
							rf.nextIndex[server] = reply.XIndex
						} else {
							rf.nextIndex[server] = rf.lastSnapshotIndex + idx
						}
					} 
				}
			}

			Assert(rf.nextIndex[server] > 0 && rf.nextIndex[server] <= rf.lastSnapshotIndex + rf.logLength() + 1, "suggest index is wrong")
			DPrintf("%v[%v %v]: %v nextindex is %v\n", rf.me, rf.currentTerm, rf.currentRole, server, rf.nextIndex[server])
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		// DPrintf("%v(%v) recv log reply %v(%v), convert to follower\n", rf.me, rf.currentTerm, follower, reply.Term)
		rf.convertToFollower()
		rf.persist()
	}
	// DPrintf("%v(%v) deal append entries reply done\n", rf.me, rf.currentTerm)
}


func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotRequest) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v[%v %v]: send snapshot to %v...\n", rf.me, rf.currentTerm, rf.currentRole, server)

	if !ok {
		DPrintf("%v[%v %v]: send snapshot to %v failed\n", rf.me, rf.currentTerm, rf.currentRole, server)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertToFollower()
		rf.resetTimeout()
		rf.currentTerm = reply.Term
		rf.persist()
	} else {
		rf.nextIndex[server] = rf.nextIndex[rf.me]
	}
}

func (rf *Raft)broadcastHeartBeat() {
	if rf.currentRole != Leader {
		return
	}
	DPrintf("%v[%v %v]: broadcast log \n", rf.me, rf.currentTerm, rf.currentRole)

	Assert(rf.me==rf.currentLeader, "only leader can replicate log\n")
	log_len := rf.logLength()

	for follower := 0; follower < rf.n && !rf.killed(); follower++ {
		if follower == rf.me {
			continue
		}
		
		if rf.nextIndex[follower] <= rf.lastSnapshotIndex {
			args := InstallSnapshotRequest{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastSnapshotIndex, LastIncludedTerm: rf.lastSnapshotTerm}
			args.Data = rf.snapshot
			
			go rf.sendInstallSnapshot(follower, args)
		} else {
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
			// Assert(args.PrevLogIndex >= 0 && args.PrevLogIndex - rf.lastSnapshotIndex <= log_len, "PrevLogIndex greater out of index\n")

			args.PrevLogIndex = rf.nextIndex[follower] - 1
			if args.PrevLogIndex - rf.lastSnapshotIndex > 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex - rf.lastSnapshotIndex].Term 	
			} else {
				args.PrevLogTerm = rf.lastSnapshotTerm
			}

			// leader不能复制不包含当前term的entry的log
			// 但是已经提交的可以
			lastEntryIndex := func() int {
				if rf.log[log_len].Term == rf.currentTerm { 
					return log_len + rf.lastSnapshotIndex
				} else {
					return rf.commitIndex
				}
			}()
			
			n := lastEntryIndex - rf.nextIndex[follower] + 1
			if n > 0 {
				// args.Entries = make([]LogEntry, n)
				args.Entries = rf.log[rf.nextIndex[follower] - rf.lastSnapshotIndex : lastEntryIndex + 1 - rf.lastSnapshotIndex]
				// copy(args.Entries, rf.log[rf.nextIndex[follower] - rf.lastSnapshotIndex : lastEntryIndex + 1 - rf.lastSnapshotIndex])
			} else {
				n = 0
			}
			
			
			go rf.sendAppendEntries(follower, args) 
		}
	}
}

func (rf *Raft)startElection() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	if rf.killed() || rf.currentRole == Leader {
		return
	}

	rf.currentTerm++
	rf.currentRole = Candidate
	rf.votedFor = rf.me
	rf.votesReceived[rf.me] = true
	rf.votesNumber = 1
	rf.persist()
	DPrintf("%v[%v %v]: start election\n", rf.me, rf.currentTerm, rf.currentRole)

	lastTerm := 0
	log_len := rf.logLength()
	if log_len > 0 {
		lastTerm = rf.log[log_len].Term
	} else {
		lastTerm = rf.lastSnapshotTerm
	}

	for follower, _ := range rf.peers {
		if follower == rf.me {
			continue
		}
		// 通过协程并行发送投票请求
		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastSnapshotIndex + log_len, LastLogTerm: lastTerm}
		go rf.sendRequestVote(follower, args)
	}
	rf.resetTimeout()
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.currentRole != Leader {
		isLeader = false
	} else {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		index = rf.logLength() + rf.lastSnapshotIndex
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		term = int(rf.currentTerm)
		rf.persist()
		rf.broadcastHeartBeat()
		DPrintf("%v[%v %v]: start cmd %v(%v)\n", rf.me, rf.currentTerm, rf.currentRole, command, index)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func electionTimeout() int64 {
	return 500 + (rand.Int63() % 500)
}

func heartbeatTimeout() int64 {
	return 70 + (rand.Int63() % 35)
}

func getNow() int64 {
	return time.Now().UnixMilli()
}


func (rf *Raft) ticker() {
	var isLeader bool

	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		// DPrintf("%v ticker\n", rf.me)
		rf.mu.Lock()

			// start leader election
		isLeader = rf.currentRole == Leader
		if isLeader {
			rf.broadcastHeartBeat()
		} else {
			if getNow() >= rf.Timeout {
				rf.startElection()
			}
		} 

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// 万恶之源，如果是leader不能sleep，会导致heartbeat间隔边长
		// 工作的协程数量降低，导致3D的时间过长
		// 间接可能导致其他server“造反”选举新的leader……
		// 这里选择将leader的心跳间隔加在这里
		// 同时如果role不是leader则定期发起选举时间是否过期的检查，也算一举两得
		// if !isLeader {
		time.Sleep(time.Duration(heartbeatTimeout()) * time.Millisecond)
		// }
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.n = len(rf.peers)

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.currentRole   = Follower
	rf.currentLeader = -1
	rf.votesReceived =  make([]bool, rf.n)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex  = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 需要修改状态，因此在初始化中执行而不是ticker
	rf.resetTimeout()

	// go func() {
	// 	log.Println(http.ListenAndServe(":6061", nil))
	// 	}()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()

	return rf
}
