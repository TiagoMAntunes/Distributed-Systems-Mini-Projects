package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrNotCommitted = "ErrNotCommitted"
	ErrNotUpdated   = "ErrNotUpdated"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64
	ClientId  int64
	Config    shardctrler.Config
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64
	ClientId  int64
	Config    shardctrler.Config
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardsArgs struct {
	Config shardctrler.Config
	Shards []int
}

type GetShardsReply struct {
	Err      Err
	Data     map[string]string
	Requests map[int64]int64
}
