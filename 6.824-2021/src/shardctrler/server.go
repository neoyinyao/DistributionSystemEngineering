package shardctrler

import (
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	killed bool
	// Your data here.
	clientState map[int64]int
	configs     []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Id        int64
	SerialNum int
	Op        string
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}
func (sc *ShardCtrler) initialClientStateWithLock(id int64){
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, ok := sc.clientState[id]
	if !ok {
		sc.clientState[id] = 0
	}
}
func (sc *ShardCtrler) reBalance(gIdAssignNum map[int]int) map[int][]int {
	DPrintf("sc[%v] reBalance gIdAssignNum : %v",sc.me,gIdAssignNum)
	var moveShards []int
	currConfig := sc.configs[len(sc.configs)-1]
	DPrintf("sc[%v] reBalance currConfig : %v",sc.me,currConfig)
	currGidAssign := make(map[int][]int)
	for shard, gid := range currConfig.Shards {
		_, ok := currGidAssign[gid]
		if ok {
			currGidAssign[gid] = append(currGidAssign[gid], shard)
		} else {
			currGidAssign[gid] = []int{shard}
		}
	}
	var currGIds []int
	for Gid,_ := range currGidAssign{
		sort.Ints(currGidAssign[Gid])
		currGIds = append(currGIds,Gid)
	}
	sort.Ints(currGIds)
	for _, gId := range currGIds {
		gidShards := currGidAssign[gId]
		assignNum, ok := gIdAssignNum[gId]
		if !ok {
			moveShards = append(moveShards, gidShards...)
			delete(currGidAssign, gId)
		} else {
			if assignNum < len(gidShards) {
				moveShards = append(moveShards,gidShards[assignNum:]...)
				currGidAssign[gId] =gidShards[:assignNum]
			}
		}
	}
	DPrintf("sc[%v] movedShards : %v",sc.me,moveShards)
	DPrintf("sc[%v] currGidAssign : %v",sc.me,currGidAssign)
	i := 0
	var gIdAssign []int
	for gId := range gIdAssignNum{
		gIdAssign = append(gIdAssign,gId)
	}
	sort.Ints(gIdAssign)
	for _, gId := range gIdAssign {
		assignNum := gIdAssignNum[gId]
		gidShards, ok := currGidAssign[gId]
		if !ok {
			currGidAssign[gId] = moveShards[i : i+assignNum]
			i += assignNum
		}else{
			if assignNum > len(gidShards) {
				addShardsNum := assignNum - len(gidShards)
				currGidAssign[gId] = append(currGidAssign[gId], moveShards[i:i+addShardsNum]...)
				i += addShardsNum
			}
		}
	}
	if i != len(moveShards) && i != 0{
		panic("i != len(currConfig.Shards)")
	}
	return currGidAssign
}
func (sc *ShardCtrler) doJoin(servers map[int][]string) {
	var availableGIds []int
	currConfig := sc.configs[len(sc.configs)-1]
	for gId := range currConfig.Groups {
		availableGIds = append(availableGIds, gId)
	}
	for gid := range servers {
		availableGIds = append(availableGIds, gid)
	}
	shouldSchedule := NShards / len(availableGIds)
	idleSchedule := NShards % len(availableGIds)
	shouldGIdAssignNum := make(map[int]int)
	sort.Ints(availableGIds)
	for _, GId := range availableGIds {
		shouldGIdAssignNum[GId] = shouldSchedule
		if idleSchedule > 0 {
			shouldGIdAssignNum[GId] += 1
			idleSchedule -= 1
		}
	}
	shouldGidAssign := sc.reBalance(shouldGIdAssignNum)
	shards := [NShards]int{}
	for gId, gIdShards := range shouldGidAssign {
		for _, shard := range gIdShards {
			shards[shard] = gId
		}
	}
	groups := make(map[int][]string)
	currGroups := currConfig.Groups
	for gId, group := range currGroups {
		groups[gId] = group
	}
	for gId, group := range servers {
		groups[gId] = group
	}
	newConfig := Config{
		Num:    currConfig.Num + 1,
		Shards: shards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) doLeave(GIds []int) {
	var availableGIds []int
	currConfig := sc.configs[len(sc.configs)-1]
	for gId := range currConfig.Groups {
		in := false
		for _,leaveGid := range GIds{
			if gId == leaveGid{
				in = true
				break
			}
		}
		if !in{
			availableGIds = append(availableGIds, gId)
		}
	}
	sort.Ints(availableGIds)
	var shouldSchedule int
	var idleSchedule int
	if len(availableGIds) == 0{
		shouldSchedule = NShards
		idleSchedule = 0
	} else {
		shouldSchedule = NShards / len(availableGIds)
		idleSchedule = NShards % len(availableGIds)
	}
	gIdAssignNum := make(map[int]int)
	for _, GId := range availableGIds {
		gIdAssignNum[GId] = shouldSchedule
		if idleSchedule > 0 {
			gIdAssignNum[GId] += 1
			idleSchedule -= 1
		}
	}
	gidAssign := sc.reBalance(gIdAssignNum)
	shards := [NShards]int{}
	for gId, gIdShards := range gidAssign {
		for _, shard := range gIdShards {
			shards[shard] = gId
		}
	}
	groups := make(map[int][]string)
	currGroups := currConfig.Groups
	for gId, group := range currGroups {
		in := false
		for _,leaveGid := range GIds{
			if gId == leaveGid{
				in = true
				break
			}
		}
		if !in{
			groups[gId] = group
		}
	}
	newConfig := Config{
		Num:    currConfig.Num + 1,
		Shards: shards,
		Groups: groups,
	}
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) doMove(shard int,GId int){
	var shards [NShards]int
	groups := make(map[int][]string)
	currConfig := sc.configs[len(sc.configs)-1]
	for assignShard,shardGId := range currConfig.Shards{
		shards[assignShard] = shardGId
	}
	shards[shard] = GId
	for gId,group := range currConfig.Groups{
		groups[gId] = group
	}
	newConfig := Config{
		Num:    currConfig.Num+1,
		Shards: shards,
		Groups: groups,
	}
	sc.configs = append(sc.configs,newConfig)
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Id:        args.Id,
		SerialNum: args.SerialNum,
		Op:        JOIN,
		Servers:   args.Servers,
		GIDs:      nil,
		Shard:     0,
		GID:       0,
		Num:       0,
	}
	_, _, ok := sc.rf.Start(op)
	if ok {
		sc.initialClientStateWithLock(args.Id)
		for {
			_, ok := sc.rf.GetState()
			if !ok {
				reply.WrongLeader = true
				return
			} else {
				sc.mu.Lock()
				if sc.clientState[args.Id] >= args.SerialNum {
					reply.WrongLeader = false
					sc.mu.Unlock()
					return
				}
				sc.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Id:        args.Id,
		SerialNum: args.SerialNum,
		Op:        LEAVE,
		Servers:   nil,
		GIDs:      args.GIDs,
		Shard:     0,
		GID:       0,
		Num:       0,
	}
	_, _, ok := sc.rf.Start(op)
	if ok {
		sc.initialClientStateWithLock(args.Id)
		for {
			_, ok := sc.rf.GetState()
			if !ok {
				reply.WrongLeader = true
				return
			} else {
				sc.mu.Lock()
				if sc.clientState[args.Id] >= args.SerialNum {
					reply.WrongLeader = false
					sc.mu.Unlock()
					return
				}
				sc.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Id:        args.Id,
		SerialNum: args.SerialNum,
		Op:        MOVE,
		Servers:   nil,
		GIDs:      nil,
		Shard:     args.Shard,
		GID:       args.GID,
		Num:       0,
	}
	_, _, ok := sc.rf.Start(op)
	if ok {
		sc.initialClientStateWithLock(args.Id)
		for {
			_, ok := sc.rf.GetState()
			if !ok {
				reply.WrongLeader = true
				return
			} else {
				sc.mu.Lock()
				if sc.clientState[args.Id] >= args.SerialNum {
					reply.WrongLeader = false
					sc.mu.Unlock()
					return
				}
				sc.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		reply.WrongLeader = true
		return
	}
}
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Id:        args.Id,
		SerialNum: args.SerialNum,
		Op:        QUERY,
		Servers:   nil,
		GIDs:      nil,
		Shard:     0,
		GID:       0,
		Num:       args.Num,
	}
	_, _, ok := sc.rf.Start(op)
	if ok {
		sc.initialClientStateWithLock(args.Id)
		for {
			_, ok := sc.rf.GetState()
			if !ok {
				reply.WrongLeader = true
				return
			} else {
				sc.mu.Lock()
				if sc.clientState[args.Id] >= args.SerialNum {
					if args.Num == -1 || args.Num > sc.configs[len(sc.configs)-1].Num{
						reply.Config = sc.configs[len(sc.configs)-1]
					}else {
						reply.Config = sc.configs[args.Num]
					}
					reply.WrongLeader = false
					DPrintf("reply config:%v",reply.Config)
					sc.mu.Unlock()
					return
				}
				sc.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) consumeApplyMsg() {
	for !sc.isKilled(){
		applyMsg := <-sc.rf.ApplyCh
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			sc.mu.Lock()
			_, ok := sc.clientState[op.Id]
			if !ok {
				sc.clientState[op.Id] = 0
			}
			if op.SerialNum <= sc.clientState[op.Id] {
				DPrintf("OpSerialNum : %v,clientState : %v",op.SerialNum,sc.clientState[op.Id])
				DPrintf("old rpc")
				sc.mu.Unlock()
			} else if op.SerialNum-sc.clientState[op.Id] > 1 {
				DPrintf("OpSerialNum : %v,clientState : %v",op.SerialNum,sc.clientState[op.Id])
				panic("SerialNum should be monotonically")
			} else {
				DPrintf("sc[%v] consumeApplyMsg op:%v",sc.me,&op)
				switch op.Op {
				case JOIN:
					sc.doJoin(op.Servers)
				case LEAVE:
					sc.doLeave(op.GIDs)
				case MOVE:
					sc.doMove(op.Shard,op.GID)
				case QUERY:
				default:
					panic("false Op")
				}
				sc.clientState[op.Id] = op.SerialNum
				DPrintf("sc[%v] last config : %v",sc.me,sc.configs[len(sc.configs)-1])
				sc.mu.Unlock()
			}
		} else {
			panic("false applyMsg.CommandValid")
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.killed = true
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
func (sc *ShardCtrler) isKilled() bool{
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.killed
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.clientState = make(map[int64]int)
	go sc.consumeApplyMsg()
	// Your code here.

	return sc
}
