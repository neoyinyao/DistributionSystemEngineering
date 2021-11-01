package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandValid bool
	ClientId     int64
	SerialNum    int
	ShardsNum    int
	Op           string
	Key          string
	Value        string

	SnapshotValid bool
	Snapshot      []byte

	ConfigValid bool
	Config      shardctrler.Config
}

type ShardState struct {
	mu sync.Mutex
	ConfigNum   int
	ClientState map[int64]int
	ServerState map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk
	shardsState  [shardctrler.NShards]ShardState
	currConfig       shardctrler.Config
	movedShardsConfigNum	 [shardctrler.NShards]int
	killed       bool
	lastApplied  int
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.shardsState[args.ShardNum].mu.Lock()
	if kv.shardsState[args.ShardNum].ConfigNum != args.ConfigNum {
		reply.Err = ErrWrongGroup
	}
	kv.shardsState[args.ShardNum].mu.Unlock()
	op := Op{
		CommandValid:  true,
		ClientId:      args.Id,
		SerialNum:     args.SerialNum,
		ShardsNum:     args.ShardNum,
		Op:            GET,
		Key:           args.Key,
		Value:         "",
		SnapshotValid: false,
		Snapshot:      nil,
		ConfigValid:   false,
		Config:        shardctrler.Config{},
	}
	_,_,isLeader := kv.rf.Start(op)
	if isLeader{
		for  {
			kv.shardsState[args.ShardNum].mu.Lock()
			if kv.shardsState[args.ShardNum].ConfigNum != args.ConfigNum{
				kv.shardsState[args.ShardNum].mu.Unlock()
				reply.Err = ErrWrongGroup
				return
			}else{
				_,isLeader = kv.rf.GetState()
				if isLeader{
					if kv.shardsState[args.ShardNum].ClientState[args.Id] >= args.SerialNum{
						value,ok := kv.shardsState[args.ShardNum].ServerState[args.Key]
						if ok{
							reply.Err = OK
							reply.Value = value
						}else {
							reply.Err = ErrNoKey
						}
						kv.shardsState[args.ShardNum].mu.Unlock()
						return
					}else {
						kv.shardsState[args.ShardNum].mu.Unlock()
						time.Sleep(10 * time.Millisecond)
					}
				}else {
					reply.Err = ErrWrongLeader
					kv.shardsState[args.ShardNum].mu.Unlock()
					return
				}
			}
		}
	}else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.killed = true
	// Your code here, if desired.
}
func (kv *ShardKV) IsKilled() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.killed
}
func (kv *ShardKV) pollConfig() {
	//long loop,only leader poll config,config do log replication
	for kv.IsKilled() {
		_, ok := kv.rf.GetState()
		if ok {
			currConfigNum := kv.currConfig.Num
			nextConfig := kv.mck.Query(currConfigNum + 1)
			if nextConfig.Num > currConfigNum {
				allShardsUpToDate := true
				for _, shardState := range kv.shardsState {
					if shardState.ConfigNum != currConfigNum {
						allShardsUpToDate = false
						break
					}
				}
				if allShardsUpToDate {
					for shard := range kv.currConfig.Shards{
						if kv.currConfig.Shards[shard] == kv.gid && nextConfig.Shards[shard] != kv.gid{
							kv.movedShardsConfigNum[shard] = currConfigNum
						}else if kv.currConfig.Shards[shard] == kv.gid && nextConfig.Shards[shard] == kv.gid{
							kv.shardsState[shard].ConfigNum = nextConfig.Num
							kv.movedShardsConfigNum[shard] = nextConfig.Num
						}else if kv.currConfig.Shards[shard] != kv.gid && nextConfig.Shards[shard] != kv.gid{
							kv.shardsState[shard].ConfigNum = nextConfig.Num
							kv.movedShardsConfigNum[shard] = nextConfig.Num
						}else {
							kv.movedShardsConfigNum[shard] = nextConfig.Num
						}
					}
					kv.currConfig = nextConfig
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs,reply *MoveShardReply) {
	//leader move shards between group,after receive group accept all,delete old shards
	kv.mu.Lock()
	kv.shardsState[args.ShardNum].mu.Lock()
	defer kv.shardsState[args.ShardNum].mu.Unlock()
	defer kv.mu.Unlock()
	if kv.currConfig.Num == args.ConfigNum {
		if kv.shardsState[args.ShardNum].ConfigNum == args.ConfigNum-1 {
			kv.shardsState[args.ShardNum].ConfigNum = args.ConfigNum
			kv.shardsState[args.ShardNum].ServerState = args.ServerState
			kv.shardsState[args.ShardNum].ClientState = args.ClientState
		} else if kv.shardsState[args.ShardNum].ConfigNum == args.ConfigNum{

		}else {
			panic("Unexpected ConfigNum")
		}
		reply.Err = OK
	}
}

func (kv *ShardKV) SendMoveShards(){
	for !kv.IsKilled(){
		_,isLeader := kv.rf.GetState()
		if isLeader{
			for shard := range kv.movedShardsConfigNum{
				if kv.movedShardsConfigNum[shard] != kv.currConfig.Num{
					gid := kv.currConfig.Shards[shard]
					if servers, ok := kv.currConfig.Groups[gid]; ok {
						// try each server for the shard.
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							args := MoveShardArgs{
								ConfigNum:   0,
								ClientState: nil,
								ServerState: nil,
								ShardNum:    0,
							}
							var reply GetReply
							ok := srv.Call("ShardKV.MoveShard", &args, &reply)
							if ok && reply.Err == OK {
								return reply.Value
							}
							// ... not ok, or ErrWrongLeader
						}
					}
				}
			}
		}else {
			break
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
