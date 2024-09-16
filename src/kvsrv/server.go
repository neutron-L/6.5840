package kvsrv

import (
	"log"
	"sync"
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
	Seen 	map[uint32]uint32    	// 客户id->最新请求seq
	Acks    map[uint32]uint32    	// 客户端最近ack的序号
	History map[uint32][]string 	// 历史记录
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
	if args.Seqno < kv.Seen[args.ClientId] {

	}
	kv.Store[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	origin := ""
	if value, ok := kv.Store[args.Key]; ok {
		reply.Value = value
		origin = value
	}
	kv.Store[args.Key] = origin + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.Store = make(map[string]string)
	kv.Store = make(map[uint64]uint64)
	return kv
}
