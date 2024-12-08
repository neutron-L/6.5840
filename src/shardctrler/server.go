package shardctrler


import (
    "6.5840/raft"
    "6.5840/labrpc"
	"sync"
	"sync/atomic"
    "6.5840/labgob"
	"log"
	"time"
	"reflect"
	"sort"
)


const Debug = false
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
	isEmpty			bool	
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
		config.Groups[gid] = append([]string(nil), arr...)
	}
	copy(config.Shards[:], origin_config.Shards[:])

	return config
}

func (sc *ShardCtrler) mostShardsGID() int {
	maxn_gid := 0
	for i, j := range sc.shardNums {
		if maxn_gid == 0 || j > sc.shardNums[maxn_gid] || (j == sc.shardNums[maxn_gid] && i < maxn_gid) {
			maxn_gid = i
		}
	}

	return maxn_gid
}


func (sc *ShardCtrler) leastShardsGID(exclude int) int {
	minn_gid := 0
	for i, j := range sc.shardNums {
		if i == exclude {
			continue
		}
		if minn_gid == 0 || j < sc.shardNums[minn_gid] || (j == sc.shardNums[minn_gid] && i < minn_gid) {
			minn_gid = i
		}
	}

	return minn_gid
}

// 返回值为错误码
func (sc *ShardCtrler) handleReq(op Op) (Err, Config) {
	var cid int64
	var seqno int64
	var args = op.Args.(ReqArgs)

	cid, seqno = args.GetClientId(), args.GetSeqno()
	
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, _, isLeader := sc.Raft().Start(op)

	if !isLeader {
		DPrintf("[%v] Server[%v]->Client[%v]: got %v not leader", seqno, sc.me, cid, op.Operation)

		return ErrWrongLeader, Config{}
	}

	DPrintf("[%v] Server[%v]: got %v from Client[%v], wait commit", seqno, sc.me, op.Operation, cid)
	

	for !sc.killed() {
		// 条件变量等待被唤醒
		sc.condDict[op.Operation].Wait()

		if sc.killed() {
			return ErrWrongLeader, Config{Num: -1}
		} 
		
		if _, isLeader := sc.Raft().GetState(); !isLeader {
			return ErrWrongLeader, Config{Num: -1}
		} 

		// 判断是否当前request被执行
		if record, ok := sc.History[cid]; !ok || record.Seqno < seqno {
			if ok {
				DPrintf("[%v] Server[%v]: wait %v entry.Seqno = %v cid = %v", 
					seqno, sc.me, op.Operation, record.Seqno, cid)
			}
			continue
		} else {
			DPrintf("[%v] Server[%v]->Client[%v]: %v reply err %v", seqno, sc.me, cid, op.Operation, record.Err)
			return record.Err, record.Config
		}
	}

	return ErrWrongLeader, Config{Num: -1}
}

func (sc *ShardCtrler) doJoin(servers map[int][]string) Err {
	config := ConfigClone(sc.configs[sc.nextCfgIdx - 1])
	config.Num = sc.nextCfgIdx

	Assert(config.Num == sc.configs[sc.nextCfgIdx - 1].Num + 1, "the config num is not increase")

	sc.nextCfgIdx++

	// 合并servers
	for gid, sg := range servers {
		_, ok := config.Groups[gid]
		Assert(!ok, "Duplicate server group")
		if gid >= 1 && gid <= 3 {
			DPrintf("Join %v", gid)
		}
		config.Groups[gid] = sg
	}

	// 获取所有的键
	keys := make([]int, 0, len(servers))
	for key := range servers {
		keys = append(keys, key)
	}

	// 按键排序
	sort.Ints(keys)

	DPrintf("Servers[%v]: Join %v", sc.me, keys)

	if sc.isEmpty {
		num_of_sg := len(servers)
		size := NShards / num_of_sg
		rem := NShards % num_of_sg
		i := 0   // 第几个sg
		start := 0  // 开始索引

		for _, gid := range keys {
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
		Assert(start == NShards, "start should equal to NShards")
	} else {
		for _, gid := range keys {
			// 找出最多shard的sg
			maxn_gid := sc.mostShardsGID()

			sc.shardNums[gid] = sc.shardNums[maxn_gid] / 2

			if sc.shardNums[gid] == 0 {
				continue
			}
			// 分一半给新的sg
			sc.shardNums[maxn_gid] -= sc.shardNums[gid]
			Assert(sc.shardNums[maxn_gid] >= 0, "less than 0")
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
		if sum != NShards {
			DPrintf("num %v sum %v NShards %v", config.Num, sum, NShards)
		}
		Assert(sum == NShards || sum == 0, "the sum of shard nums is not equal to NShard or 0")
		if len(sc.shardNums) != len(config.Groups) {
			DPrintf("shard Nums")
			DPrintf("%v", servers)
			DPrintf("%v", sc.shardNums)
			DPrintf("%v", config.Groups)
		}
		Assert(len(sc.shardNums) == len(config.Groups), "the number of sg is not consistent")

	}

	sc.configs = append(sc.configs, config)
	sc.isEmpty = false
	Assert(config.Num == len(sc.configs) - 1, "Wrong Num")


	DPrintf("=============================");
	DPrintf("shardNum: %v", sc.shardNums)
	DPrintf("config[%v]: Shard %v %v", config.Num, config.Shards, config.Groups)


	return OK
}


func (sc *ShardCtrler) doLeave(GIDs []int) Err {
	config := ConfigClone(sc.configs[sc.nextCfgIdx - 1])
	config.Num = sc.nextCfgIdx

	Assert(config.Num == sc.configs[sc.nextCfgIdx - 1].Num + 1, "the config num is not increase")

	sc.nextCfgIdx++

	DPrintf("Servers[%v]: Leave %v", sc.me, GIDs)


	for _, GID := range GIDs {
		if GID >= 1 && GID <= 3 {
			DPrintf("Leave %v", GID)
		}
		delete(config.Groups, GID)
		// 找到目前shard最少的SG
		minn_gid := sc.leastShardsGID(GID)

		if minn_gid != 0 {
			sc.shardNums[minn_gid] += sc.shardNums[GID]
		} else {
			DPrintf("only GID %v %v", GID, len(sc.shardNums))
			DPrintf("%v", sc.shardNums)
		}

		for i, g := range config.Shards {
			if g == GID {
				config.Shards[i] = minn_gid
			}
		}

		delete(sc.shardNums, GID)

		if minn_gid == 0 {
			sc.isEmpty = true
			if len(sc.shardNums) != 0 {
				DPrintf("%v", sc.shardNums)
				Assert(false, "fuck minn gid")
			}
			DPrintf("config shards %v", config.Shards)
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
		if sum != NShards {
			sc.isEmpty = true
			DPrintf("num %v sum %v NShards %v", config.Num, sum, NShards)
		}
		Assert(sum == NShards || sum == 0, "the sum of shard nums is not equal to NShard or 0")
		Assert(len(sc.shardNums) == len(config.Groups), "the number of sg is not consistent")
		if sum == 0 {
			Assert(sc.isEmpty && len(sc.shardNums) == 0, "empty assert failed")
		}
	}


	sc.configs = append(sc.configs, config)
	Assert(config.Num == len(sc.configs) - 1, "Wrong Num")

	DPrintf("=============================");
	DPrintf("shardNum: %v", sc.shardNums)
	DPrintf("config[%v]: Shard %v %v", config.Num, config.Shards, config.Groups)

	return OK
}

func (sc *ShardCtrler) doMove(shard int, GID int) Err {
	config := ConfigClone(sc.configs[sc.nextCfgIdx - 1])
	config.Num = sc.nextCfgIdx

	Assert(config.Num == sc.configs[sc.nextCfgIdx - 1].Num + 1, "the config num is not increase")

	sc.nextCfgIdx++

	old_gid := config.Shards[shard]
	sc.shardNums[old_gid]--
	sc.shardNums[GID]++
	config.Shards[shard] = GID

	sc.configs = append(sc.configs, config)
	Assert(config.Num == len(sc.configs) - 1, "Wrong Num")

	DPrintf("=============================");
	DPrintf("shardNum: %v", sc.shardNums)
	DPrintf("config[%v]: Shard %v %v", config.Num, config.Shards, config.Groups)

	return OK
}


func (sc *ShardCtrler) doQuery(num int) (Err, Config) {
	DPrintf("Server[%v]: Query %v", sc.me, num)
	// if  {
	// 	DPrintf("Server[%v]: Query %v > next index %v", sc.me, num, sc.nextCfgIdx)
	// 	return ErrWrongLeader, Config{Num: -1}
	// }
	if num == -1 || num >= sc.nextCfgIdx {
		DPrintf("Server[%v]: Query change num %v -> %v", sc.me, num, sc.nextCfgIdx - 1)
		num = sc.nextCfgIdx - 1
	}
	if num < 0 {
		return ErrNoExist, Config{}
	}
	
	Assert(num == sc.configs[num].Num, "num is not consistent")

	return OK, sc.configs[num]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err, _ = sc.handleReq(Op{Operation: JOIN, Args: args, ClientId: args.ClientId, Seqno: args.Seqno})
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err, _ = sc.handleReq(Op{Operation: LEAVE, Args: args})
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err, _ = sc.handleReq(Op{Operation: MOVE, Args: args})
	reply.WrongLeader = reply.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err, reply.Config = sc.handleReq(Op{Operation: QUERY, Args: args})
	reply.WrongLeader = reply.Err == ErrWrongLeader
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
						DPrintf("msg %v", msg)
						DPrintf("fuck %v", reflect.TypeOf(msg.Command))
						sc.mu.Unlock()
						continue
					}
					DPrintf("reflect type %v", reflect.TypeOf(op.Args))
					args = op.Args.(ReqArgs)

					cid, seqno = args.GetClientId(), args.GetSeqno()
		
					if record, ok := sc.History[cid]; ok && seqno <= record.Seqno {
						DPrintf("[%v]executed: record.cid = %v; record.Seqno = %v; op.Seqno = %v", sc.me, cid, record.Seqno, seqno)
					} else {
						if !ok {
							record = Record{}
						}
						record.Seqno = seqno
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
							DPrintf("Server[%v]: Query cid %v seno %v num %v", sc.me, cid, seqno, args.(*QueryArgs).Num)
							record.Err, record.Config = sc.doQuery(args.(*QueryArgs).Num)
						}
						}
		
						sc.History[cid] = record
						DPrintf("Server[%v]: execute client[%v][%v]", sc.me, cid, record.Seqno)
					}
		
					sc.condDict[op.Operation].Broadcast()
					Assert(sc.lastApplied < msg.CommandIndex, "apply a old command")
					DPrintf("Server[%v]: update lastApplied to command index %v -> %v, nextidx %v", sc.me, sc.lastApplied, msg.CommandIndex, sc.nextCfgIdx)

					sc.lastApplied = msg.CommandIndex
				} else if msg.SnapshotValid {
					DPrintf("what snap")
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
	sc.Raft().Kill()
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
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})
	labgob.Register(&QueryArgs{})

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
	sc.isEmpty = true

	go sc.executeLoop()
    

	return sc
}
