package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
	// "fmt"
)


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId 	int64
	Seqno	 	uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.Seqno = 1

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key}
	reply := GetReply{}

	ok := ck.server.Call("KVServer.Get", &args, &reply)
	if ok {
		return reply.Value
	}

	// 定义一个定时器，这里用time.Tick创建一个每秒触发的定时器作为示例  
	// 注意：time.Tick不会停止，除非我们手动停止它，但这里我们使用它来模拟每次重试的间隔  
	ticker := time.Tick(1 * time.Second)  
  
	// 使用for循环来管理重试逻辑  
	for {  
		select {  
		case <-ticker:  
			ok = ck.server.Call("KVServer.Get", &args, &reply)
			if ok {
				return reply.Value
			}
		}  
	}  
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.ClientId, Seqno: ck.Seqno}
	reply := PutAppendReply{}
	ck.Seqno++

	ok := ck.server.Call("KVServer."+op, &args, &reply)
	if ok {
		return reply.Value
	} 
  
	// 定义一个定时器，这里用time.Tick创建一个每秒触发的定时器作为示例  
	// 注意：time.Tick不会停止，除非我们手动停止它，但这里我们使用它来模拟每次重试的间隔  
	ticker := time.Tick(1 * time.Second)  
  
	// 使用for循环来管理重试逻辑  
	for {  
		select {  
		case <-ticker:  
			ok = ck.server.Call("KVServer."+op, &args, &reply)
			if ok {
				return reply.Value
			} 
		}  
	}  
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
