package kvraft

import (
	"bytes"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"os"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


// 自定义的assert，可用性欠佳
func Assert(condition bool, msg string) {
	if Debug && !condition {
		log.Fatalf("Assertion failed: %s", msg)
	}
}

// 枚举类型，Get/Put/Append
const (
    GET    	  string = "Get" 
    PUT    	  string = "Put" 
    APPEND    string = "Append" 
)

const Delay = 400

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType			string 
	Key				string
	Value 			string

	ClientId 		int64
	Seqno   		int64
}

type Record	struct {
	Seqno   		int64
	Key				string
	Value			string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Store		map[string]string
	History		map[int64]Record   // 对于每个client，处理过的请求的序号
	
	condDict	map[string]*sync.Cond  // Op type -> cond
	putCond		*sync.Cond
	appendCond	*sync.Cond

	// Snapshot相关
	persister *raft.Persister          // Object to hold this peer's persisted state
	lastApplied   int
	snapshotCond *sync.Cond
}


func (kv *KVServer) applySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.Store) != nil || d.Decode(&kv.History) != nil || d.Decode(&kv.lastApplied) != nil {
		DPrintf("readPersist error %v\n", kv.persister.SnapshotSize())
		os.Exit(1)
	} 
}


func (kv *KVServer) snapshotLoop() {
	for !kv.killed() {
		kv.mu.Lock()

		for kv.persister.RaftStateSize() < kv.maxraftstate {
			kv.snapshotCond.Wait()
		}
		DPrintf("Server[%v]: snapshot %v", kv.me, kv.lastApplied)
		// build snapshot
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.Store)
		e.Encode(kv.History)
		e.Encode(kv.lastApplied)

		kv.rf.Snapshot(kv.lastApplied, w.Bytes())

		kv.mu.Unlock()
	}
}

func (kv *KVServer) handleReq(op Op) (Err, string) {
	// Your code here.
	var err Err = OK
	value := ""

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// op := Op{OpType: GET, Key: args.Key, ClientId: args.ClientId, Seqno: args.Seqno}
	// index, term, isLeader := kv.rf.Start(op)
	_, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		err = ErrWrongLeader
		DPrintf("[%v] Server[%v]->Client[%v]: got %v not leader", op.Seqno, kv.me, op.ClientId, op.OpType)
		return err, value
	}

	DPrintf("[%v] Server[%v]: got %v from Client[%v], wait commit", op.Seqno, kv.me, op.OpType, op.ClientId)

	// 使用for循环来管理重试逻辑  
	for !kv.killed() {  
		kv.condDict[op.OpType].Wait()

		// 检查是否alive
		if kv.killed() {
			return err, value
		}

		//  检查leader资格
		if _, flag := kv.rf.GetState(); !flag {
			err = ErrWrongLeader
			DPrintf("[%v] Server[%v]->Client[%v]: got %v not leader", op.Seqno, kv.me, op.ClientId, op.OpType)

			return err, value
		}

		// 检查处理历史
		if entry, ok := kv.History[op.ClientId]; !ok || entry.Seqno < op.Seqno {
			if ok {
				DPrintf("[%v] Server[%v]: wait %v entry.Seqno = %v; args.Seqno = %v", 
					op.Seqno, kv.me, op.OpType, entry.Seqno, op.Seqno)
			}
			
			continue
		}
		// Assert(kv.History[args.ClientId].OpType == Get, "history optype error") 
		// Assert(kv.History[args.ClientId].Seqno == args.Seqno, "history seqno error") 
		// Assert(kv.History[args.ClientId].Key == args.Key, "history key error") 
		err = OK
		if op.OpType == GET {
			value = kv.History[op.ClientId].Value
		}
		DPrintf("[%v] Server[%v]->Client[%v]: %v reply", op.Seqno, kv.me, op.ClientId, op.OpType)

		return err, value
	}  
	return err, value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OpType: GET, Key: args.Key, ClientId: args.ClientId, Seqno: args.Seqno}

	reply.Err, reply.Value = kv.handleReq(op)  
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OpType: PUT, Key: args.Key, Value: args.Value, ClientId: args.ClientId, Seqno: args.Seqno}
	reply.Err, _ = kv.handleReq(op)  
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OpType: APPEND, Key: args.Key, Value: args.Value, ClientId: args.ClientId, Seqno: args.Seqno}
	reply.Err, _ = kv.handleReq(op)  
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 不断从applyChan中取出已经提交的Op并执行
func (kv *KVServer) executeLoop() {
	for !kv.killed() {
		select {
		case msg:=<-kv.applyCh:
			{
				kv.mu.Lock()

				if msg.CommandValid {
					op, ok := msg.Command.(Op)
					if !ok {
						kv.mu.Unlock()
						continue
					}
		
					// Assert(kv.History[op.ClientId].Seqno <= op.Seqno, "")
					entry, ok := kv.History[op.ClientId]
					if ok && entry.Seqno >= op.Seqno {
						DPrintf("[%v]executed: entry.cid = %v; entry.Seqno = %v; op.Seqno = %v", kv.me, op.ClientId, entry.Seqno, op.Seqno)
					} else {
						if !ok {
							entry = Record{}
						} 
						
						entry.Seqno = op.Seqno
						entry.Key = op.Key
	
						DPrintf("[%v]execute [%v]%v: entry.cid = %v; entry.Seqno = %v; op.Seqno = %v", kv.me, msg.CommandIndex, op.OpType, op.ClientId, entry.Seqno, op.Seqno)

						if op.OpType == PUT {
							kv.Store[op.Key] = op.Value
						} else if op.OpType == APPEND {
							kv.Store[op.Key] += op.Value
						}

						entry.Value = kv.Store[op.Key]
						kv.History[op.ClientId] = entry
					}
					kv.condDict[op.OpType].Broadcast()

					if kv.lastApplied < msg.CommandIndex {		
						DPrintf("Server[%v]: update lastApplied to command index %v -> %v", kv.me, kv.lastApplied, msg.CommandIndex)
						kv.lastApplied = msg.CommandIndex	
						// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
						// 	kv.snapshotCond.Signal()
						// }
					} 
					
					
				} else {
					Assert(msg.SnapshotValid, "invalid snapshot and invalid command")
					if kv.lastApplied < msg.SnapshotIndex {
						oldApplied := kv.lastApplied
						kv.applySnapshot(msg.Snapshot)
						kv.lastApplied = msg.SnapshotIndex
						DPrintf("Server[%v]: update lastApplied to snapshot index %v -> %v", kv.me, oldApplied, kv.lastApplied)
					} else {
						DPrintf("Server[%v]: recv a old snapshot index %v -> %v", kv.me, kv.lastApplied, msg.SnapshotIndex)
					}
				}
				// 将snapshot的条件检查移出，4B最后一个case耗时60-90s左右
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					kv.snapshotCond.Signal()
				}
				kv.mu.Unlock()
				break
			}
		case <-time.After(Delay * time.Millisecond):
			DPrintf("[%v]execute: check alive", kv.me)
			
			if kv.killed() {
				for _, cond := range kv.condDict {
					cond.Broadcast()
				}
				return
			}
		}
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.Store = make(map[string]string)
	kv.History = make(map[int64]Record)

	kv.condDict = make(map[string]*sync.Cond)
	kv.condDict[GET] = sync.NewCond(&kv.mu)
	kv.condDict[PUT] = sync.NewCond(&kv.mu)
	kv.condDict[APPEND] = sync.NewCond(&kv.mu)

	kv.lastApplied = 0
	kv.persister = persister
	kv.snapshotCond = sync.NewCond(&kv.mu)

	// 读取状态
	if persister.SnapshotSize() > 0 {
		kv.applySnapshot(persister.ReadSnapshot())
	}

	go kv.executeLoop()

	if maxraftstate != -1 {
		go kv.snapshotLoop()
	}

	return kv
}
