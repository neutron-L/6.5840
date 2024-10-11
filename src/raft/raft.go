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
	"time"
	"6.5840/labgob"
	"6.5840/labrpc"
	"fmt"
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

type Role int32
 
const (  
    Follower    Role = iota // 0  
    Candidate               // 1  
    Leader                  // 2  
)  

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	n		  int
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

	// 当状态发生变化时，如选票数足够以及角色变化，则通过其发送信号给ticker协程
	sigChan				chan struct{}

	// 传送apply Msg的chan
	applyCh				chan ApplyMsg

	// snapshot相关
	lastSnapshotIndex	int
	lastSnapshotTerm	int
	snapshot			[]byte

	TimeBefore          int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	DPrintf("%v want get state\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get state lock\n", rf.me)

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

	if rf.lastSnapshotIndex != 0 && d.Decode(&rf.snapshot) != nil {
		Assert(false, "readPersist error\n")
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	DPrintf("%v start snapshot %v\n", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v(%v) snapshot %v\n", rf.me, rf.currentTerm, index)

	Assert(index <= rf.commitIndex, "snapshot index is greater than commit index\n")
	rf.snapshot = snapshot
	rf.lastSnapshotTerm = rf.log[index - rf.lastSnapshotIndex].Term

	log_len := rf.logLength()
	old_log := rf.log[index - rf.lastSnapshotIndex + 1:]
	rf.log = make([]LogEntry, log_len - (index - rf.lastSnapshotIndex) + 1)
	copy(rf.log[1:], old_log[:])
	rf.lastSnapshotIndex = index

	// 更新commit & apply 下标
	if rf.commitIndex < index {
		rf.commitIndex = index
		rf.lastApplied = index
	}
	DPrintf("%v(%v) snapshot done\n", rf.me, rf.currentTerm)

	// DPrintf("%v(%v) index %v term %v\n", rf.me, rf.currentTerm, rf.lastSnapshotIndex, rf.lastSnapshotTerm)
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

	// 状态发生改变
	go func(me int) { rf.sigChan <- struct{}{}
	DPrintf("%v change to follower\n",me)
	} (rf.me)
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
			rf.matchIndex[follower] = log_len
		}
	}

	// 状态发生改变
	go func() { rf.sigChan <- struct{}{} } ()
}


// 返回日志长度，论文中日志的下标从1开始，为了方便修改，将日志长度计算封装
// 调用时需要已经持有rf的锁
func (rf *Raft)logLength() int {
	return len(rf.log) - 1
}

func (rf *Raft)apply(n int, arr []ApplyMsg) {
	for _, msg := range arr {
		rf.applyCh <- msg
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied += n
}

// 检查哪些entry可以提交，调用时需要已经持有rf的锁
func (rf *Raft)commitLogEntries() {
	replicateNum := 0
	i := rf.commitIndex + 1
	log_len := rf.logLength()

	// for j := 0; j < rf.n; j++ {
	// 	if rf.matchIndex[j] >= rf.commitIndex {
	// 		replicateNum++
	// 	}
	// }
	
	// if replicateNum < (followerNum + 1) / 2 {
	// 	DPrintf("prev commit index %v\n", rf.commitIndex)
	// 	Assert(false, "when commit log, prev commitIndex is wrong\n")
	// }

	for i <= log_len + rf.lastSnapshotIndex {
		replicateNum = 0
		for j := 0; j < rf.n; j++ {
			if rf.matchIndex[j] >= i {
				replicateNum++
			}
		}
		
		if replicateNum < (rf.n + 1) / 2 {
			break
		}
		i++
	}
	i--
	
	DPrintf("%v apply\n", rf.me)
	if i > rf.commitIndex && rf.log[i - rf.lastSnapshotIndex].Term == rf.currentTerm {
		msgArr := make([]ApplyMsg, i - rf.commitIndex)
		for j := rf.commitIndex + 1; j <= i; j++ {
			msgArr = append(msgArr, ApplyMsg{CommandValid: true, Command: rf.log[j - rf.lastSnapshotIndex].Command, CommandIndex: j}) 
		}
		go rf.apply(i - rf.commitIndex, msgArr[:])
		rf.commitIndex = i

		// rf.lastApplied = i

		DPrintf("%v(%v) update commit index %v\n", rf.me, rf.currentTerm, i)
	} 
	DPrintf("%v apply done\n", rf.me)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// stateChanged := false
	// DPrintf("RequestVote: %v(%v) -> %v\n", args.CandidateId, args.Term, rf.me)
	DPrintf("%v rpc want call request vote -> %v\n", args.CandidateId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v rpc get call request vote lock -> %v\n", args.CandidateId, rf.me)

	// DPrintf("%v(%v %v) recv request vote from %v(%v)\n", rf.me, rf.currentTerm, rf.currentRole, args.CandidateId, args.Term)

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
		
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("%v(%v) vote to %v(%v)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	} else if !logOk {
		DPrintf("%v(%v) refuse vote to %v(%v), log len %v last term %v, args log idx %v log term %v\n", rf.me, rf.currentTerm, args.CandidateId, args.Term,
		log_len, lastTerm, args.LastLogIndex, args.LastLogTerm)
	} else {
		DPrintf("%v(%v) refuse vote to %v(%v), has vote for %v\n", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor)
	}
	reply.Term = rf.currentTerm
}

// leader复制日志到其他机器或者发送空日志作为heartbeat信息
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	needPersistence := false

	DPrintf("%v rpc want call append entries -> %v\n", args.LeaderId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v rpc get call append entries lock -> %v\n", args.LeaderId, rf.me)

	if rf.currentTerm < args.Term  {
		rf.currentTerm = args.Term
		needPersistence = true
	}

	if rf.currentTerm == args.Term {
		rf.convertToFollower()
		rf.currentLeader = args.LeaderId
	}

	log_len := rf.logLength()
	logOk := (log_len + rf.lastSnapshotIndex >= args.PrevLogIndex) && 
			(args.PrevLogIndex == rf.lastSnapshotIndex && args.PrevLogTerm == rf.lastSnapshotTerm || args.PrevLogIndex > rf.lastSnapshotIndex && rf.log[args.PrevLogIndex- rf.lastSnapshotIndex].Term == args.PrevLogTerm)

	if !logOk {
		DPrintf("%v !logOk: log len %v, snapidx %v, snapterm %v; previdx %v prev term %v\n", rf.me, log_len, rf.lastSnapshotIndex, rf.lastSnapshotTerm, args.PrevLogIndex, args.PrevLogTerm)
	}

	if rf.currentTerm == args.Term && logOk {
		// 复制log
		suffix_len := len(args.Entries)
		if suffix_len > 0 {
			DPrintf("%v recv: %v entries: %v\n", rf.me, args.PrevLogIndex, args.Entries)
		}
		if suffix_len > 0 && log_len + rf.lastSnapshotIndex > args.PrevLogIndex  {
			index := log_len + rf.lastSnapshotIndex
			if args.PrevLogIndex + suffix_len < log_len + rf.lastSnapshotIndex {
				index = args.PrevLogIndex + suffix_len
			}
			DPrintf("two term %v(%v) %v(%v) \n", args.Entries[index - args.PrevLogIndex - 1], args.Entries[index - args.PrevLogIndex - 1].Term, rf.log[index - rf.lastSnapshotIndex], rf.log[index - rf.lastSnapshotIndex].Term)
			if args.Entries[index - args.PrevLogIndex - 1].Term != rf.log[index - rf.lastSnapshotIndex].Term {
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
		DPrintf("%v(%v) recv log commit %d new cmtidx %v log len %v suffix_len %v args.PrevLogIndex %v snapidx %v\n", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit, rf.logLength(), suffix_len, args.PrevLogIndex, rf.lastSnapshotIndex)
		if rf.commitIndex < args.LeaderCommit && args.LeaderCommit <= rf.lastSnapshotIndex + rf.logLength() {
			msgArr := make([]ApplyMsg, args.LeaderCommit - rf.commitIndex)
			Assert(rf.commitIndex >= rf.lastSnapshotIndex, "commit index should not less than lastsnapshot index\n")
			for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
				msgArr = append(msgArr, ApplyMsg{CommandValid: true, Command: rf.log[i - rf.lastSnapshotIndex].Command, CommandIndex: i}) 
			}
			go rf.apply(args.LeaderCommit - rf.commitIndex, msgArr)

			rf.commitIndex = args.LeaderCommit
			// rf.lastApplied = args.LeaderCommit
			needPersistence = true
		}
		

		reply.Term = rf.currentTerm
		reply.Success = true
		reply.Ack = suffix_len + args.PrevLogIndex + 1
		DPrintf("%v(%v) recv log, new commit %d  suffix_len %v args.PrevLogIndex %v ack %d\n", rf.me, rf.currentTerm, rf.commitIndex, suffix_len, args.PrevLogIndex, reply.Ack)
	} else {
		if rf.currentTerm == args.Term {
			reply.XLen = rf.lastSnapshotIndex + rf.logLength()

			if args.PrevLogIndex > rf.lastSnapshotIndex && args.PrevLogIndex <= rf.lastSnapshotIndex + rf.logLength() {
				reply.XTerm = rf.log[args.PrevLogIndex - rf.lastSnapshotIndex].Term

				i := args.PrevLogIndex
				for i > 1 + rf.lastSnapshotIndex && rf.log[i - rf.lastSnapshotIndex].Term == rf.log[i - 1 - rf.lastSnapshotIndex].Term  {
					i--
				}
				reply.XIndex = i
				DPrintf("%v(%v) snapshot idx %v xlen %v xterm %v xindex %v myterm %v prevterm %v\n", rf.me, rf.currentTerm, rf.lastSnapshotIndex, reply.XLen, reply.XTerm, reply.XIndex, rf.log[args.PrevLogIndex - rf.lastSnapshotIndex].Term, args.PrevLogTerm)
			} else {
				reply.XSnapshotIndex = rf.lastSnapshotIndex
				reply.XSnapshotTerm = rf.lastSnapshotTerm
			}
		} else {
			DPrintf("%v reject log %v \n", rf.currentTerm, args.Term)
		} 
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	
	// DPrintf("%v(%v) new commit %d log len %v\n", rf.me, rf.currentTerm, rf.commitIndex, rf.logLength())

	if needPersistence {
		rf.persist()
	}
}



func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	needPersistence := false

	DPrintf("%v rpc want call install snapshot -> %v\n", args.LeaderId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v rpc get call install snapshot lock -> %v\n", args.LeaderId, rf.me)

	if rf.currentTerm < args.Term  {
		rf.currentTerm = args.Term
	}

	if rf.currentTerm == args.Term {
		rf.convertToFollower()
		rf.currentLeader = args.LeaderId
		rf.currentTerm = args.Term
		needPersistence = true
	}
	reply.Term = rf.currentTerm

	DPrintf("%v(%v) snapidx %v install snapshot idx %v\n", rf.me, rf.currentTerm, rf.lastSnapshotIndex, args.LastIncludedIndex)

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
		rf.lastApplied = args.LastIncludedIndex
	}

	// 先持久化
	if needPersistence {
		rf.persist()
	}

	go func() {
		DPrintf("want apply snapshot\n")
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("get apply snapshot lock\n")

		
		rf.applyCh <- ApplyMsg {SnapshotValid: true, Snapshot: rf.snapshot, SnapshotTerm: rf.lastSnapshotTerm, SnapshotIndex: rf.lastSnapshotIndex}
	}()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	DPrintf("%v want deal request vote reply -> %v\n", rf.me, server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get request vote reply lock -> %v\n", rf.me, server)



	if !ok {
		DPrintf("%v(%v) send request vote to %v failed\n", rf.me, rf.currentTerm, server)
		return
	}

	// 不仅需要调用成功且得到投票，还需要判断是否是当前任期内发起的vote
	if rf.currentRole == Candidate && reply.VoteGranted && reply.Term == rf.currentTerm {
		if !rf.votesReceived[server] {
			DPrintf("%v(%v) recv vote from %v, now %v\n", rf.me, rf.currentTerm, server, rf.votesNumber + 1)
			rf.votesReceived[server] = true
			rf.votesNumber++
			if rf.votesNumber >= (rf.n + 1) / 2 {
				DPrintf("New leader is %v(%v)\n", rf.me, rf.currentTerm)

				rf.convertToLeader()
			}
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		DPrintf("%v(%v) recv vote reply %v(%v), convert to follower\n", rf.me, rf.currentTerm, server, reply.Term)
		rf.convertToFollower()
		rf.persist()
	} else if rf.currentRole != Candidate {
		DPrintf("%v(%v) is %v\n", rf.me, rf.currentTerm, rf.currentRole)
	} 
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		DPrintf("%v send log failed\n", rf.me)
		return
	}
	
	DPrintf("%v want deal send log reply -> %v\n", rf.me, server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get send log reply lock -> %v\n", rf.me, server)

	DPrintf("%v(%v) deal append entries reply\n", rf.me, rf.currentTerm)
	log_len := rf.logLength()
	if reply.Term == rf.currentTerm && rf.currentRole == Leader {
		Assert(rf.me == rf.currentLeader, "Leader is me\n")
		if reply.Success && reply.Ack > rf.matchIndex[server] {
			DPrintf("%v commit log\n", rf.me)
			// DPrintf("%v(%v) recv replicate reply from %v ack %v\n", rf.me, rf.currentTerm, follower, reply.Ack)
			// Assert(rf.nextIndex[follower] <= rf.lastSnapshotIndex + log_len + 1, "Ack out of index\n")
			rf.nextIndex[server] = reply.Ack
			rf.matchIndex[server] = reply.Ack - 1
			Assert(rf.nextIndex[server] > 0 && rf.nextIndex[server] <= rf.lastSnapshotIndex + log_len + 1, "ack is wrong")
			// commit log entries
			rf.commitLogEntries()
			DPrintf("%v commit log done\n", rf.me)
		} else {
			// DPrintf("%v(%v) recv suggest index %v for %v\n", rf.me, rf.currentTerm, reply.SuggestIndex, follower)
			// rf.nextIndex[follower] = reply.SuggestIndex
			DPrintf("%v compute nextindex\n", rf.me)

			if reply.XSnapshotIndex != 0 {
				rf.nextIndex[server] = reply.XSnapshotIndex + 1
			} else {
				if rf.nextIndex[server] > reply.XLen + 1 {
					rf.nextIndex[server] = reply.XLen + 1 
				} else {
					// 查找第一个大于xterm的term的下标，二分法
					log_len := rf.logLength()
					l := 1
					r := log_len + 1

					for l < r {
						mid := (l + r) >> 1
						if rf.log[mid].Term <= reply.XTerm {
							l = mid + 1
						} else {
							r = mid
						}
					}

					if r == 1 {
						rf.nextIndex[server] = 1 + rf.lastSnapshotIndex
					} else {
						if rf.log[r - 1].Term != reply.XTerm {
							rf.nextIndex[server] = reply.XIndex
						} else {
							rf.nextIndex[server] = rf.lastSnapshotIndex + r - 1
						}
					} 
				}
			}

			Assert(rf.nextIndex[server] > 0 && rf.nextIndex[server] <= rf.lastSnapshotIndex + rf.logLength() + 1, "suggest index is wrong")
			DPrintf("%v compute nextindex done\n", rf.me)
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		// DPrintf("%v(%v) recv log reply %v(%v), convert to follower\n", rf.me, rf.currentTerm, follower, reply.Term)
		rf.convertToFollower()
		rf.persist()
	}
	DPrintf("%v(%v) deal append entries reply done\n", rf.me, rf.currentTerm)
}


func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if !ok {
		DPrintf("%v send snapshot %v failed\n", rf.me, server)
		return
	}

	DPrintf("%v want deal install snap reply -> %v\n", rf.me, server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get install snap reply lock -> %v\n", rf.me, server)
	if reply.Term > rf.currentTerm {
		rf.convertToFollower()
		rf.currentTerm = reply.Term
		rf.persist()
	} else {
		rf.nextIndex[server] = rf.nextIndex[rf.me]
		DPrintf("%v send snapshot to %v succ %v\n", rf.me, server, rf.nextIndex[rf.me])
	}
}

func (rf *Raft)broadcastHeartBeat() {
	DPrintf("%v broadcast log \n", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("%v(%v) duration %v\n", rf.me, rf.currentRole, time.Now().UnixMilli() - rf.TimeBefore)
	rf.TimeBefore = time.Now().UnixMilli()
	if rf.currentRole != Leader {
		return
	}
	Assert(rf.me==rf.currentLeader, "only leader can replicate log\n")
	for follower := 0; follower < rf.n; follower++ {
		if follower == rf.me {
			continue
		}
		// 开启一个协程发送并处理返回值
		go rf.replicateLog(follower)
	}
	DPrintf("%v broadcast log done \n", rf.me)
}

func (rf *Raft)startElection() {
	needPersistence := false

	DPrintf("%v want to election...\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole == Leader {
		return
	}

	DPrintf("%v start election %v\n", rf.me, rf.currentTerm+1)
	rf.currentTerm++
	rf.currentRole = Candidate
	rf.votedFor = rf.me
	rf.votesReceived[rf.me] = true
	rf.votesNumber = 1
	needPersistence = true

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
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastSnapshotIndex + log_len, LastLogTerm: lastTerm}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(follower, args, reply)
	}
	if needPersistence {
		rf.persist()
	}
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
	DPrintf("%v want to start %v\n", rf.me, command)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get lock for %v\n", rf.me, command)
	
	if rf.currentRole != Leader {
		isLeader = false
	} else {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		index = rf.logLength() + rf.lastSnapshotIndex
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		term = int(rf.currentTerm)
		DPrintf("Leader %v(%v) cmd %v(%v)\n", rf.me, rf.currentTerm, command, index)
		// go rf.broadcastHeartBeat()
		rf.persist()
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

func electionTimeout() int {
	return int(500 + (rand.Int63() % 500))
}

func heartbeatTimeout() int {
	return int( 70 + (rand.Int63() % 35))
}

func (rf *Raft)replicateLog(follower int) {
	DPrintf("%v want replicate log -> %v\n", rf.me, follower)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v get rep log lock -> %v\n", rf.me, follower)

	if rf.currentRole != Leader {
		return
	}
	Assert(rf.me == rf.currentLeader, "only leader can replicate log\n")

	// 发送snapshot替代
	if rf.nextIndex[follower] <= rf.lastSnapshotIndex {
		args := &InstallSnapshotRequest{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastSnapshotIndex, LastIncludedTerm: rf.lastSnapshotTerm}
		args.Data = make([]byte, len(rf.snapshot))
		copy(args.Data, rf.snapshot)
		DPrintf("%v(%v) send snapshot to %v, next Index %v snapshot index %v\n", rf.me, rf.currentTerm, follower, rf.nextIndex[follower], rf.lastSnapshotIndex)
		reply := &InstallSnapshotReply{}
		
		go rf.sendInstallSnapshot(follower, args, reply)

		return
	}

	log_len := rf.logLength()
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
	Assert(args.PrevLogIndex >= 0 && args.PrevLogIndex - rf.lastSnapshotIndex <= log_len, "PrevLogIndex greater out of index\n")

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
	// DPrintf("nextidx %v loglen %v lastidx %v comidx %v\n", rf.nextIndex[follower], log_len, rf.lastSnapshotIndex, rf.commitIndex)
	// for i := rf.nextIndex[follower]; i <= lastEntryIndex; i++ {
	// 	args.Entries = append(args.Entries, rf.log[i - rf.lastSnapshotIndex])
	// }
	n := lastEntryIndex - rf.nextIndex[follower] + 1
	if n > 0 {
		args.Entries = make([]LogEntry, n)
		copy(args.Entries, rf.log[rf.nextIndex[follower] - rf.lastSnapshotIndex : lastEntryIndex + 1 - rf.lastSnapshotIndex])
	} else {
		n = 0
	}
	
	DPrintf("%v(%v) -> %v entries %v\n", rf.me, rf.currentTerm, follower, len(args.Entries))
	Assert(len(args.Entries) + args.PrevLogIndex <= rf.lastSnapshotIndex + log_len, "set args error")

	DPrintf("%v(%v %v) send log to %v, prev index %v entry %v\n", rf.me, rf.currentTerm, rf.currentRole, follower, args.PrevLogIndex, len(args.Entries))
	reply := &AppendEntriesReply{}
	
	go rf.sendAppendEntries(follower, args, reply) 
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(time.Duration(electionTimeout())  * time.Millisecond)  

	var set_  int64

	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// for {
			// 读取当前状态
			_, isLeader := rf.GetState()
			delay := func() int {  
				if isLeader {
					return heartbeatTimeout()
				} else {
					return electionTimeout()
				}
			}()
			if isLeader {
				set_ = time.Now().UnixMilli()
				fmt.Printf("%d timeout after %v\n", rf.me, delay)
			}
			timer.Reset(time.Duration(delay) * time.Millisecond)

			if (!isLeader) {
				select {
				case <-timer.C:
					// fmt.Printf("T %d: %d restart election\n", rf.currentTerm, rf.me)
					select {
					case <-rf.sigChan:
						DPrintf("%v timer and sigchan\n", rf.me)
						if _, isLeader := rf.GetState(); isLeader {
							// fmt.Printf("T %d: %d is leader\n", rf.currentTerm, rf.me)
							go rf.broadcastHeartBeat()
						}
					default:
						go rf.startElection()
					}
				case <-rf.sigChan:
					// 如果成为leader
					// timer.Stop()
					DPrintf("%v got sig\n", rf.me)
					if _, isLeader := rf.GetState(); isLeader {
						// fmt.Printf("T %d: %d is leader\n", rf.currentTerm, rf.me)
						go rf.broadcastHeartBeat()
					}
					
				}
			} else {
				// fmt.Printf("T %d: %d is leader\n", rf.currentTerm, rf.me)

				select {
				case <-timer.C:
					// leader广播heartbeat消息
					// timer.Stop()
					fmt.Printf("%v broadcast timeout %v\n", rf.me, time.Now().UnixMilli() - set_)
					go rf.broadcastHeartBeat()

				case <-rf.sigChan:
					// timer.Stop()
					// DPrintf("T %d: %d %d get sigChan\n", rf.currentTerm, rf.currentRole, rf.me)
				}
			}
		// }
			timer.Stop()

		// pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
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

	rf.sigChan = make(chan struct{})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
