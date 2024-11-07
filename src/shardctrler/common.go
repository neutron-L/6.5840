package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type ReqArgs interface {
	GetClientId() int64
	GetSeqno() int64
}


type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ClientId 		int64
	Seqno	 		int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ClientId 		int64
	Seqno	 		int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientId 		int64
	Seqno	 		int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	ClientId 		int64
	Seqno	 		int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}


func (args *JoinArgs) GetClientId() int 64 {
	return args.ClientId
}


func (args *JoinArgs) GetSeqno() int 64 {
	return args.Seqno
}


func (args *LeaveArgs) GetClientId() int 64 {
	return args.ClientId
}


func (args *LeaveArgs) GetSeqno() int 64 {
	return args.Seqno
}


func (args *MoveArgs) GetClientId() int 64 {
	return args.ClientId
}


func (args *MoveArgs) GetSeqno() int 64 {
	return args.Seqno
}


func (args *QueryArgs) GetClientId() int 64 {
	return args.ClientId
}


func (args *QueryArgs) GetSeqno() int 64 {
	return args.Seqno
}