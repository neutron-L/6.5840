package shardctrler


import (
    "6.5840/raft"
    "6.5840/labrpc"
	"sync"
	"sync/atomic"
    "6.5840/labgob"
	"log"
	"time"
)


const Debug = true
const Delay = 400

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Record struct {
	Seqno int64
	Err	 Err
	Config Config
}

type OpType string


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead	int32

	// Your data here.
	lastApplied  	int
	History			map[int64]Record             // 每个client的已处理序号
	condDict		map[OpType]*sync.Cond
	configs 		[]Config // indexed by config num
	nextCfgIdx		int

	// 记录每个sg拥有的shard数量
	shardNums		map[int]int
}



// 枚举类型，Get/Put/Append
const (
    JOIN    OpType = "Join" 
    LEAVE   OpType = "Leave" 
    MOVE    OpType = "Move" 
    QUERY   OpType = "Query" 
)



type Op struct {
	// Your data here.
	Operation	OpType
	Args        ReqArgs

	ClientId 		int64
	Seqno	 		int64
}



func Assert(condition bool, msg string) {
	if !condition {
		log.Fatalf("Assertion failed: %s", msg)
	}
}

func ConfigClone(origin_config Config) Config {
	config := Config{Num: origin_config.Num}

	config.Groups = make(map[int][]string)

	for gid, arr := range origin_config.Groups {
		config.Groups[gid] = make([]string, 0)
		copy(config.Groups[gid], arr)
	}
	copy(config.Shards[:], origin_config.Shards[:])

	return config
}

func (sc *ShardCtrler) mostShardsGID() int {
	maxn_gid := 0
	for i, j := range sc.shardNums {
		if maxn_gid == 0 || j > sc.shardNums[maxn_gid] {
			maxn_gid = i
		}
	}

	return maxn_gid
}


func (sc *ShardCtrler) leastShardsGID() int {
	minn_gid := 0
	for i, j := range sc.shardNums {
		if minn_gid == 0 || j < sc.shardNums[minn_gid] {
			minn_gid = i
		}
	}

	return minn_gid
}

// 返回值为错误码
func (sc *ShardCtrler) handleReq(op Op) (Err, Config) {
	var cid int64
	var seqno int64

	var args = op.Args
	cid, seqno = args.GetClientId(), args.GetSeqno()
	
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, _, isLeader := sc.rf.Start(op)


	args = op.Args
	cid, seqno = args.GetClientId(), args.GetSeqno()
		

	if !isLeader {
		DPrintf("[%v] Server[%v]->Client[%v]: got %v not leader", seqno, sc.me, cid, op.Operation)

		return ErrWrongLeader, Config{}
	}

	DPrintf("[%v] Server[%v]: got %v from Client[%v], wait commit", seqno, sc.me, op.Operation, cid)
	

	for !sc.killed() {
		// 条件变量等待被唤醒
		sc.condDict[op.Operation].Wait()

		if sc.killed() {
			return ErrWrongLeader, Config{}
		} 
		
		if _, isLeader := sc.rf.GetState(); !isLeader {
			return ErrWrongLeader, Config{}
		} 

		// 判断是否当前request被执行
		if record, ok := sc.History[cid]; !ok || record.Seqno < seqno {
			if ok {
				DPrintf("[%v] Server[%v]: wait %v entry.Seqno = %v cid = %v", 
					seqno, sc.me, op.Operation, record.Seqno, cid)
			}
			continue
		} else {
			DPrintf("[%v] Server[%v]->Client[%v]: %v reply", seqno, sc.me, cid, op.Operation)
			return OK, record.Config
		}
	}

	return ErrWrongLeader, Config{}
}

func (sc *ShardCtrler) doJoin(servers map[int][]string) Err {
	config := ConfigClone(sc.configs[sc.nextCfgIdx - 1])
	config.Num = sc.nextCfgIdx
	sc.nextCfgIdx++

	Assert(config.Num == sc.configs[sc.nextCfgIdx - 1].Num + 1, "the config num is not increase")
	// 合并servers
	for gid, sg := range servers {
		_, ok := config.Groups[gid]
		Assert(!ok, "Duplicate server group")
		config.Groups[gid] = sg
	}

	if config.Num == 1 {
		num_of_sg := len(servers)
		size := NShards / num_of_sg
		rem := NShards % num_of_sg
		i := 0   // 第几个sg
		start := 0  // 开始索引
		for gid, _ := range servers {
			sc.shardNums[gid] = size
			if i < rem {
				sc.shardNums[gid]++
			}
			for j := 0; j < sc.shardNums[gid]; j++ {
				config.Shards[start + j] = gid
			}

			start += sc.shardNums[gid]
			i++
		}
	} else {
		for gid, _ := range servers {
			// 找出最多shard的sg
			maxn_gid := sc.mostShardsGID()

			sc.shardNums[gid] = sc.shardNums[maxn_gid] / 2

			if sc.shardNums[gid] == 0 {
				break
			}
			// 分为两半给新的sg
			sc.shardNums[maxn_gid] -= sc.shardNums[gid]

			// 修改Shard
			count := 0
			for i, g := range config.Shards {
				if g == maxn_gid {
					config.Shards[i] = gid
					count++

					if count == sc.shardNums[gid] {
						break
					}
				}
			}
		}
	}

	// variant检查
	if Debug {
		sum := 0
		for gid, x := range sc.shardNums {
			s := 0
			for _, g := range config.Shards {
				if g == gid {
					s++
				}
			}
			Assert(s == x, "the number of shard is not equal to shardNums record")
			sum += x
		}
		Assert(sum == NShards, "the sum of shard nums is not equal to NShard")
		Assert(len(sc.shardNums) == len(config.Groups), "the number of sg is not consistent")
	}

	sc.configs = append(sc.configs, config)
	DPrintf("config[%v]: Shard %v", config.Num, config.Shards)

	return OK
}


func (sc *ShardCtrler) doLeave(GIDs []int) Err {
	config := ConfigClone(sc.configs[sc.nextCfgIdx - 1])
	config.Num = sc.nextCfgIdx
	sc.nextCfgIdx++

	for _, GID := range GIDs {
		delete(config.Groups, GID)
		// 找到目前shard最少的SG
		minn_gid := sc.leastShardsGID()

		sc.shardNums[minn_gid] += sc.shardNums[GID]
		delete(sc.shardNums, GID)

		for i, g := range config.Shards {
			if g == GID {
				config.Shards[i] = minn_gid
			}
		}
	}
	sc.configs = append(sc.configs, config)
	DPrintf("config[%v]: Shard %v", config.Num, config.Shards)


	return OK
}

func (sc *ShardCtrler) doMove(shard int, GID int) Err {
	config := ConfigClone(sc.configs[sc.nextCfgIdx - 1])
	config.Num = sc.nextCfgIdx
	sc.nextCfgIdx++

	old_gid := config.Shards[shard]
	sc.shardNums[old_gid]--
	sc.shardNums[GID]++
	config.Shards[shard] = GID

	sc.configs = append(sc.configs, config)
	DPrintf("config[%v]: Shard %v", config.Num, config.Shards)


	return OK
}


func (sc *ShardCtrler) doQuery(num int) (Err, Config) {
	if num == -1 {
		num = sc.nextCfgIdx - 1
	}
	if num < 0 || num >= sc.nextCfgIdx {
		return ErrNoExist, Config{}
	}
	return OK, sc.configs[num]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err, _ = sc.handleReq(Op{Operation: JOIN, Args: args, ClientId: args.ClientId, Seqno: args.Seqno})

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err, _ = sc.handleReq(Op{Operation: LEAVE, Args: args})
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err, _ = sc.handleReq(Op{Operation: MOVE, Args: args})

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err, reply.Config = sc.handleReq(Op{Operation: QUERY, Args: args})
}

func (sc *ShardCtrler) executeLoop() {
	var args ReqArgs
	var cid   int64
	var seqno int64

	for !sc.killed() {
		select {
			case msg := <-sc.applyCh: {
				sc.mu.Lock()
				if msg.CommandValid {
					op, ok := msg.Command.(Op)
					if !ok {
						sc.mu.Unlock()
						continue
					}
					args = op.Args
					cid, seqno = args.GetClientId(), args.GetSeqno()
		
					if record, ok := sc.History[cid]; ok && seqno <= record.Seqno {
						DPrintf("[%v]executed: record.cid = %v; record.Seqno = %v; op.Seqno = %v", sc.me, cid, record.Seqno, seqno)
					} else {
						if !ok {
							record = Record{Seqno: seqno}
						}
						switch op.Operation {
						case JOIN: {
							record.Err = sc.doJoin(args.(*JoinArgs).Servers)
						}
						case LEAVE: {
							record.Err = sc.doLeave(args.(*LeaveArgs).GIDs)
						}
						case MOVE: {
							record.Err = sc.doMove(args.(*MoveArgs).Shard, args.(*MoveArgs).GID)
						}
						case QUERY: {
							record.Err, record.Config = sc.doQuery(args.(*QueryArgs).Num)
						}
						}
		
						sc.History[cid] = record
					}
		
					sc.condDict[op.Operation].Broadcast()
					Assert(sc.lastApplied < msg.CommandIndex, "apply a old command")
					sc.lastApplied = msg.CommandIndex
				} else if msg.SnapshotValid {
		
				}
				sc.mu.Unlock()
			}
			case <-time.After(Delay * time.Millisecond): 
				DPrintf("[%v]execute: check alive", sc.me)
		
				if sc.killed() {
					for _, cond := range sc.condDict {
						cond.Broadcast()
					}
					return
				}
		}
	}
	
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	// 两条语句不能反过来，否则rf已经killed了但是sc还是alive的
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
	// Your code here, if desired.
}


// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.nextCfgIdx = 1

	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.History	= make(map[int64]Record)

	sc.condDict = make(map[OpType]*sync.Cond)
	sc.condDict[JOIN]  = sync.NewCond(&sc.mu)
	sc.condDict[LEAVE] = sync.NewCond(&sc.mu)
	sc.condDict[MOVE]  = sync.NewCond(&sc.mu)
	sc.condDict[QUERY] = sync.NewCond(&sc.mu)

	sc.shardNums = make(map[int]int)
	go sc.executeLoop()
    

	return sc
}
