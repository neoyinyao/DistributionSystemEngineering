package shardmaster

//
// Shardmaster clerk.
//

import "6.824/labs/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	id int64
	seq int
	lastRequestLeader int
	// Your data here.
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
	ck.id = nrand()
	ck.seq = 0
	ck.lastRequestLeader = 0
	// Your code here.
	return ck
}
func (ck *Clerk) updateSeq(leader int){
	ck.seq ++
	ck.lastRequestLeader = leader
}
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Seq = ck.seq
	args.Id = ck.id
	lastRequestLeader := ck.lastRequestLeader
	for {
		// try each known server.
		for ;lastRequestLeader<len(ck.servers);lastRequestLeader++ {
			var reply QueryReply
			ok := ck.servers[lastRequestLeader].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.updateSeq(lastRequestLeader)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
		if lastRequestLeader == len(ck.servers){
			lastRequestLeader = 0
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Id:      ck.id,
		Seq:     ck.seq,
		Servers: servers,
	}
	// Your code here.
	lastRequestLeader := ck.lastRequestLeader
	for {
		// try each known server.
		for ;lastRequestLeader<len(ck.servers);lastRequestLeader++ {
			var reply JoinReply
			ok := ck.servers[lastRequestLeader].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.updateSeq(lastRequestLeader)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		if lastRequestLeader == len(ck.servers){
			lastRequestLeader = 0
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Seq = ck.seq
	args.Id = ck.id
	lastRequestLeader := ck.lastRequestLeader
	for {
		// try each known server.
		for ;lastRequestLeader<len(ck.servers);lastRequestLeader++ {
			var reply LeaveReply
			ok := ck.servers[lastRequestLeader].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.updateSeq(lastRequestLeader)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		if lastRequestLeader == len(ck.servers){
			lastRequestLeader = 0
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Seq = ck.seq
	args.Id = ck.id
	lastRequestLeader := ck.lastRequestLeader
	for {
		// try each known server.
		for ;lastRequestLeader<len(ck.servers);lastRequestLeader++ {
			var reply MoveReply
			ok := ck.servers[lastRequestLeader].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.updateSeq(lastRequestLeader)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
		if lastRequestLeader == len(ck.servers){
			lastRequestLeader = 0
		}
	}
}
