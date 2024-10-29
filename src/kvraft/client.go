package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"


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

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
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

	DPrintf("Client[%v]->Server[%v]: %v seqno(%v) key(%v)", ck.ClientId, ck.CurrentLeader, GET, args.Seqno, key)
	ok := ck.servers[ck.CurrentLeader].Call("KVServer.Get", &args, &reply)
	if ok {
		if reply.Err != ErrWrongLeader {
			DPrintf("Client[%v]:  %v seqno(%v) key(%v) reply %v", ck.ClientId, GET, args.Seqno, key, reply.Value)
			return reply.Value
		} 
		ck.CurrentLeader = (ck.CurrentLeader + 1) % ck.ServerNum
	}


	// 定义一个定时器，这里用time.Tick创建一个每秒触发的定时器作为示例  
	// 注意：time.Tick不会停止，除非我们手动停止它，但这里我们使用它来模拟每次重试的间隔  
	ticker := time.Tick(1 * time.Second)  
  
	// 使用for循环来管理重试逻辑  
	for {  
		select {  
		case <-ticker:  
			DPrintf("Client[%v]->Server[%v]: %v seqno(%v) key(%v)", ck.ClientId, ck.CurrentLeader, GET, ck.Seqno, key)
			ok = ck.servers[ck.CurrentLeader].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err != ErrWrongLeader {
					DPrintf("Client[%v]:  %v seqno(%v) key(%v) reply %v", ck.ClientId, GET, args.Seqno, key, reply.Value)
					return reply.Value
				} 
				ck.CurrentLeader = (ck.CurrentLeader + 1) % ck.ServerNum
			}
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

	DPrintf("Client[%v]->Server[%v]: %v seqno(%v) key(%v)", ck.ClientId, ck.CurrentLeader, op, args.Seqno, key)

	ok := ck.servers[ck.CurrentLeader].Call("KVServer." + op, &args, &reply)
	if ok {
		if reply.Err != ErrWrongLeader {
			DPrintf("Client[%v]:  %v seqno(%v) key(%v)", ck.ClientId, op, args.Seqno, key)
			return
		} 
		ck.CurrentLeader = (ck.CurrentLeader + 1) % ck.ServerNum
	}


	// 定义一个定时器，这里用time.Tick创建一个每秒触发的定时器作为示例  
	// 注意：time.Tick不会停止，除非我们手动停止它，但这里我们使用它来模拟每次重试的间隔  
	ticker := time.Tick(1 * time.Second)  
  
	// 使用for循环来管理重试逻辑  
	for {  
		select {  
		case <-ticker:  
			DPrintf("Client[%v]->Server[%v]: %v seqno(%v) key(%v)", ck.ClientId, ck.CurrentLeader, op, args.Seqno, key)
			ok = ck.servers[ck.CurrentLeader].Call("KVServer." + op, &args, &reply)
			if ok {
				if reply.Err != ErrWrongLeader {
					DPrintf("Client[%v]:  %v seqno(%v) key(%v)", ck.ClientId, op, args.Seqno, key)
					return
				} 
				ck.CurrentLeader = (ck.CurrentLeader + 1) % ck.ServerNum
			}
		}  
	}  
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
