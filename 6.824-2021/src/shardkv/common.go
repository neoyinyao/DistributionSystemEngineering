package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	GET            = "Get"
	APPEND         = "APPEND"
	PUT            = "PUT"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	Id        int64
	SerialNum int
	ConfigNum int
	ShardNum  int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	Id        int64
	SerialNum int
	ConfigNum int
	ShardNum  int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	ConfigNum  int
	ClientState map[int64]int
	ServerState map[string]string
	ShardNum   int
}
type MoveShardReply struct {
	Err Err
}
