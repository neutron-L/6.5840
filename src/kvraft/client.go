package kvraft

import (
	"sync/atomic"
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

const RetryDelay = 500


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId 		int64
	Seqno	 		int64

	CurrentLeader  	int
	ServerNum		int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var nextId int64 = 100

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// ck.ClientId = nrand()
	ck.ClientId = nextId
	nextId++
	ck.Seqno = 1
	ck.CurrentLeader = 0
	ck.ServerNum = len(servers)


	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, ClientId: ck.ClientId, Seqno: ck.Seqno}
	reply := GetReply{}
	ck.Seqno++

	type Pair struct {
		CurLeader  int
		Value      string
	}

	var done int32 = 0
	ch := make(chan Pair)
	defer close(ch)

	go func(server int, ch chan Pair, done *int32, args GetArgs, reply GetReply) {
		DPrintf("[%v] Client[%v]->Server[%v]: %v key(%v)", args.Seqno, ck.ClientId, server, GET, key)
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			DPrintf("[%v] Client[%v]: %v key(%v) reply %v", args.Seqno, ck.ClientId, GET, key, reply.Value)
			if atomic.CompareAndSwapInt32(done, 0, 1) {
				ch<-Pair{CurLeader: ck.CurrentLeader, Value: reply.Value}
			}
		} 
	}(ck.CurrentLeader, ch, &done, args, reply)

	// 使用for循环来管理重试逻辑  
	for {  
		select {  
			case item := <-ch:  
				ck.CurrentLeader = item.CurLeader

				return item.Value
			case <- time.After(RetryDelay * time.Millisecond):
				ck.CurrentLeader = (ck.CurrentLeader + 1) % ck.ServerNum
	
				go func(server int, ch chan Pair, done *int32, args GetArgs, reply GetReply) {
					DPrintf("[%v] Client[%v]->Server[%v]: %v key(%v)", args.Seqno, ck.ClientId, server, GET, key)
					ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
					if ok && reply.Err != ErrWrongLeader {
						DPrintf("[%v] Client[%v]: %v key(%v) reply %v", args.Seqno, ck.ClientId, GET, key, reply.Value)
						if atomic.CompareAndSwapInt32(done, 0, 1) {
							ch<-Pair{CurLeader: ck.CurrentLeader, Value: reply.Value}
						}
					} 
				}(ck.CurrentLeader, ch, &done, args, reply)
				break
			}  
	}  
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.ClientId, Seqno: ck.Seqno}
	reply := PutAppendReply{}
	ck.Seqno++

	var done int32 = 0 
	ch := make(chan int)
	defer close(ch)

	go func(server int, ch chan int, done *int32, args PutAppendArgs, reply PutAppendReply) {
		DPrintf("[%v] Client[%v]->Server[%v]: %v key(%v)", args.Seqno, ck.ClientId, server, op, key)
		ok := ck.servers[server].Call("KVServer." + op, &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			DPrintf("[%v] Client[%v]: %v key(%v) reply", args.Seqno, ck.ClientId, op, key)
			if atomic.CompareAndSwapInt32(done, 0, 1) {
				ch<-ck.CurrentLeader
			}
		} 
	}(ck.CurrentLeader, ch, &done, args, reply)
	
	// 使用for循环来管理重试逻辑  
	for {  
		select {  
		case server := <-ch:  
			ck.CurrentLeader = server
			return
		case <- time.After(RetryDelay * time.Millisecond):
			ck.CurrentLeader = (ck.CurrentLeader + 1) % ck.ServerNum

			go func(server int, ch chan int, done *int32, args PutAppendArgs, reply PutAppendReply) {
				DPrintf("[%v] Client[%v]->Server[%v]: %v key(%v)", args.Seqno, ck.ClientId, server, op, key)
				ok := ck.servers[server].Call("KVServer." + op, &args, &reply)
				if ok && reply.Err != ErrWrongLeader {
					DPrintf("[%v] Client[%v]: %v key(%v) reply", args.Seqno, ck.ClientId, op, key)
					if atomic.CompareAndSwapInt32(done, 0, 1) {
						ch<-ck.CurrentLeader
					}
				} 
			}(ck.CurrentLeader, ch, &done, args, reply)
			break
		}  
	}  
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
