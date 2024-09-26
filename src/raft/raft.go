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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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

	SuggestIndex	int   // log复制的时候，如果不匹配则给出一个建议值
	Ack				int   // log复制时，follower期待的下次接收的entry的下标	
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
	go func() { rf.sigChan <- struct{}{} } ()
}

// 状态转为leader

func (rf *Raft) convertToLeader() {
	rf.currentRole = Leader
	rf.currentLeader = rf.me

	// 复制日志前做初始化，正好此时持有锁
	log_len := rf.logLength()
	n := len(rf.peers)
	for follower := 0; follower < n; follower++ {
		rf.nextIndex[follower] = log_len + 1
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

// 检查哪些entry可以提交，调用时需要已经持有rf的锁
func (rf *Raft)commitLogEntries() {
	replicateNum := 0
	i := rf.commitIndex + 1
	followerNum := len(rf.peers)
	log_len := rf.logLength()

	for i <= log_len {
		replicateNum = 0
		for j := 0; j < followerNum; j++ {
			if rf.matchIndex[j] >= i {
				replicateNum++
			}
		}
		
		if replicateNum < (followerNum + 1) / 2 {
			break
		}
		i++
	}
	i--
	if i > rf.commitIndex && rf.log[i].Term == rf.currentTerm {
		for j := rf.commitIndex + 1; j <= i; j++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[j].Command, CommandIndex: j} 
		}
		rf.commitIndex = i
		rf.lastApplied = i

		DPrintf("%v(%v) update commit index %v\n", rf.me, rf.currentTerm, i)
	} 
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// stateChanged := false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v recv request vote from %v\n", rf.me, args.CandidateId)

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	lastTerm := 0
	log_len := rf.logLength()
	if log_len > 0 {
		lastTerm = rf.log[log_len].Term
	}
	logOk := args.LastLogTerm > lastTerm || 
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= log_len) 

	reply.VoteGranted = false
	if rf.currentTerm == args.Term && logOk && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
}

// leader复制日志到其他机器或者发送空日志作为heartbeat信息
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.Term  {
		rf.currentTerm = args.Term
	}

	if rf.currentTerm == args.Term {
		rf.currentLeader = args.LeaderId
		rf.convertToFollower()
	}

	log_len := rf.logLength()
	logOk := (log_len >= args.PrevLogIndex) && (args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex].Term <= args.PrevLogTerm)

	if rf.currentTerm == args.Term && logOk {
		// 复制log
		suffix_len := len(args.Entries)
		if suffix_len > 0 && log_len > args.PrevLogIndex  {
			index := log_len
			if args.PrevLogIndex + suffix_len < log_len {
				index = args.PrevLogIndex + suffix_len
			}
			if args.Entries[index - args.PrevLogIndex - 1].Term != rf.log[index].Term {
				log_len = args.PrevLogIndex
				rf.log = rf.log[:log_len + 1]
			}
		}

		if args.PrevLogIndex + suffix_len > log_len {
			for i := log_len - args.PrevLogIndex; i < suffix_len; i++ {
				rf.log = append(rf.log, args.Entries[i])
			} 
		}

		DPrintf("%v(%v) recv log commit %d  suffix_len %v args.PrevLogIndex %v\n", rf.me, rf.currentTerm, rf.commitIndex, suffix_len, args.PrevLogIndex)
		// 更新commitIndex & apply command
		if (args.LeaderCommit > rf.logLength()) {
			DPrintf("local len %v cidx %v ldidx %v\n", rf.logLength(), rf.commitIndex, args.LeaderCommit)
			Assert(false, "commit index greater than log len\n")
		}
		if rf.commitIndex < args.LeaderCommit {
			for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i} 
			}
			rf.commitIndex = args.LeaderCommit
			rf.lastApplied = args.LeaderCommit
		}
		

		reply.Term = rf.currentTerm
		reply.Success = true
		reply.Ack = suffix_len + args.PrevLogIndex + 1
		DPrintf("%v(%v) recv log commit %d  suffix_len %v args.PrevLogIndex %v ack %d\n", rf.me, rf.currentTerm, rf.commitIndex, suffix_len, args.PrevLogIndex, reply.Ack)
	} else {
		if rf.currentTerm == args.Term {
			if log_len < args.PrevLogIndex {
				reply.SuggestIndex = log_len
			} else {
				i := args.PrevLogIndex
				for i > 1 && rf.log[i].Term == rf.log[i - 1].Term  {
					i--
				}
				reply.SuggestIndex = i
				Assert(reply.SuggestIndex > 0, "suggest index must > 0")
			}
			DPrintf("%v(%v) give suggest index %v\n", rf.me, rf.currentTerm, reply.SuggestIndex)
		} else {
			DPrintf("%v reject log %v \n", rf.currentTerm, args.Term)
		}
		reply.Term = rf.currentTerm
		reply.Success = false
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft)broadcastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole != Leader {
		return
	}
	Assert(rf.me==rf.currentLeader, "only leader can replicate log\n")
	n := len(rf.peers)
	for follower := 0; follower < n; follower++ {
		if follower == rf.me {
			continue
		}
		// 开启一个协程发送并处理返回值
		go rf.replicateLog(follower)
	}
}

func (rf *Raft)startElection() {
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

	lastTerm := 0
	log_len := rf.logLength()
	if log_len > 0 {
		lastTerm = rf.log[log_len].Term
	}

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: log_len, LastLogTerm: lastTerm}
	for follower, _ := range rf.peers {
		if follower == rf.me {
			continue
		}
		// 通过协程并行发送投票请求
		go func(follower int, args *RequestVoteArgs, rf *Raft) {
			// stateChanged := false
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(follower, args, reply)

			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 不仅需要调用成功且得到投票，还需要判断是否是当前任期内发起的vote
			if rf.currentRole == Candidate && reply.VoteGranted && reply.Term == rf.currentTerm {
				if !rf.votesReceived[follower] {

					rf.votesReceived[follower] = true
					rf.votesNumber++
					if rf.votesNumber >= (len(rf.peers) + 1) / 2 {
						DPrintf("New leader is %v(%v)\n", rf.me, rf.currentTerm)

						rf.convertToLeader()
					}
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
			}
		}(follower, &args, rf)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.currentRole != Leader {
		isLeader = false
	} else {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		index = rf.logLength()
		rf.matchIndex[rf.me] = index
		term = int(rf.currentTerm)
		DPrintf("Leader %v(%v) cmd %v(%v)\n", rf.me, rf.currentTerm, command, index)
		// go rf.broadcastHeartBeat()
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
	return int(700 + (rand.Int63() % 350))
}

func heartbeatTimeout() int {
	return int( 50 + (rand.Int63() % 50))
}

func (rf *Raft)replicateLog(follower int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole != Leader {
		return
	}
	Assert(rf.me == rf.currentLeader, "only leader can replicate log\n")

	log_len := rf.logLength()
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
	args.PrevLogIndex = rf.nextIndex[follower] - 1
	Assert(args.PrevLogIndex >= 0 && args.PrevLogIndex <= log_len, "PrevLogIndex greater out of index\n")
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term 	
	args.Entries = make([]LogEntry, 0, log_len + 1 - rf.nextIndex[follower])

	// leader不能复制不包含当前term的entry的log
	// 但是已经提交的可以
	lastEntryIndex := func() int {
		if rf.log[log_len].Term == rf.currentTerm { 
			return log_len
		} else {
			return rf.commitIndex
		}
	}()

	for i := rf.nextIndex[follower]; i <= lastEntryIndex; i++ {
		args.Entries = append(args.Entries, rf.log[i])
	}
	
	Assert(len(args.Entries) + args.PrevLogIndex <= log_len, "set args error")

	DPrintf("%v(%v) send log to %v, prev index %v entry %v\n", rf.me, rf.currentTerm, follower, args.PrevLogIndex, len(args.Entries))
	go func(follower int, args *AppendEntriesArgs, rf *Raft) {
		// stateChanged := false
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(follower, args, reply)
		
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term == rf.currentTerm && rf.currentRole == Leader {
				Assert(rf.me == rf.currentLeader, "Leader is me\n")
				if reply.Success && reply.Ack >= rf.nextIndex[follower] {
					DPrintf("%v(%v) recv replicate reply from %v ack %v\n", rf.me, rf.currentTerm, follower, reply.Ack)
					Assert(rf.nextIndex[follower] <= log_len + 1, "Ack out of index\n")
					rf.nextIndex[follower] = reply.Ack
					rf.matchIndex[follower] = reply.Ack - 1

					// commit log entries
					rf.commitLogEntries()
				} else {
					Assert(reply.SuggestIndex <= log_len, "give a wrong suggest index\n")
					DPrintf("%v(%v) recv suggest index %v for %v\n", rf.me, rf.currentTerm, reply.SuggestIndex, follower)
					rf.nextIndex[follower] = reply.SuggestIndex
					// rf.nextIndex[follower]--
				}
			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.convertToFollower()
			}
		} else {
		DPrintf("%v(%v) send log to %v error\n", rf.me, rf.currentTerm, follower)
		}
	}(follower, &args, rf)
}

func (rf *Raft) ticker() {
	timer := time.NewTimer(time.Duration(electionTimeout())  * time.Millisecond)  

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
			// fmt.Printf("%d(%v) is alive\n", rf.me, isLeader)

			timer.Reset(time.Duration(delay) * time.Millisecond)
			if (!isLeader) {

				select {
				case <-timer.C:
					// fmt.Printf("T %d: %d restart election\n", rf.currentTerm, rf.me)
					go rf.startElection()
				case <-rf.sigChan:
					// 如果成为leader
					// timer.Stop()
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
			// ms := 50 + (rand.Int63() % 300)
			// time.Sleep(time.Duration(ms) * time.Millisecond)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.currentRole   = Follower
	rf.currentLeader = -1
	rf.votesReceived =  make([]bool, len(rf.peers))

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex  = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.sigChan = make(chan struct{})

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
