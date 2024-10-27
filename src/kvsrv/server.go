package kvsrv

import (
	"log"
	"sync"
	// "strconv"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Store 	map[string]string
	Seen 	map[int64]uint32    	// 客户id->最新请求seq
	// Acks    map[int64]uint32    	// 客户端最近ack的序号
	History map[int64]string 	// 历史记录，append使用，记录下标
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var ok bool
	if reply.Value, ok = kv.Store[args.Key]; !ok {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.Seen[args.ClientId]; !ok {
		// kv.History[args.ClientId] = make(map[uint32]string)
		kv.Seen[args.ClientId] = 1
	} 
	if args.Seqno >= kv.Seen[args.ClientId] {
		kv.Seen[args.ClientId] = args.Seqno + 1
		// kv.History[args.ClientId] = len(args.Value)
		kv.History[args.ClientId] = ""
		// reply.Value = kv.Store[args.Key]
		kv.Store[args.Key] = args.Value
	// delete(kv.History[args.ClientId], args.Seqno)
	} else {
		// reply.Value = kv.History[args.ClientId]
	}
	
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.Seen[args.ClientId]; !ok {
		// kv.History[args.ClientId] = make(map[uint32]string)
		kv.Seen[args.ClientId] = 1
	} 
	if args.Seqno >= kv.Seen[args.ClientId] {
		kv.Seen[args.ClientId] = args.Seqno + 1
		// kv.History[args.ClientId] = len(kv.Store[args.Key])
		kv.History[args.ClientId] = kv.Store[args.Key]
		reply.Value = kv.Store[args.Key]
		kv.Store[args.Key] = kv.Store[args.Key] + args.Value
		// delete(kv.History[args.ClientId], args.Seqno)
	} else {
		// reply.Value = kv.Store[args.Key][:kv.History[args.ClientId]]
		reply.Value = kv.History[args.ClientId]
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.Store = make(map[string]string)
	kv.Seen = make(map[int64]uint32)
	// kv.Acks = make(map[int64]uint32)
	kv.History = make(map[int64]string)

	return kv
}
